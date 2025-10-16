import json
import threading
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from config import config
from models import DriverPosition, Commentary, LeaderboardUpdate
from schemas import LEADERBOARD_UPDATE_SCHEMA, COMMENTARY_SCHEMA, SCHEMA_SUBJECTS

class KafkaConsumer:
    def __init__(self):
        kafka_config = config.get_kafka_config()
        kafka_config['group.id'] = config.get_consumer_group()
        kafka_config['auto.offset.reset'] = 'latest'
        # Ultra-aggressive consumer config for maximum real-time performance
        kafka_config['fetch.min.bytes'] = 1  # Fetch messages as soon as available
        kafka_config['fetch.wait.max.ms'] = 0  # No wait on server - immediate fetch
        kafka_config['max.partition.fetch.bytes'] = 1048576  # 1MB per partition
        kafka_config['enable.auto.commit'] = True  # Auto-commit for speed
        kafka_config['queued.max.messages.kbytes'] = 51200  # 50MB queue for high throughput
        kafka_config['session.timeout.ms'] = 30000  # 30 second session timeout
        kafka_config['heartbeat.interval.ms'] = 10000  # 10 second heartbeat
        
        self.consumer = Consumer(kafka_config)
        self.topics = list(config.get_topic_names().values())
        
        # Setup Schema Registry
        schema_registry_config = config.get_schema_registry_config()
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        
        # Setup deserializers
        self.positions_deserializer = AvroDeserializer(
            self.schema_registry_client,
            json.dumps(LEADERBOARD_UPDATE_SCHEMA),
            from_dict=self._positions_from_dict
        )
        
        # Use JSON Schema deserializer for commentary (updated schema format)
        self.commentary_deserializer = JSONDeserializer(
            json.dumps(COMMENTARY_SCHEMA),
            from_dict=self._commentary_from_dict
        )
        
        # In-memory storage for latest data
        self.latest_positions = []  # Legacy - for backward compatibility
        self.race_positions = {}  # New - grouped by race_id
        self.latest_commentary = []
        self.position_callbacks = []
        self.commentary_callbacks = []
        
        self.running = False
        self.consumer_thread = None
    
    def _positions_from_dict(self, obj, ctx):
        """Convert dictionary to DriverPosition object"""
        if obj is None:
            return None
        
        return {
            'driver_name': obj['driver_name'],
            'position': obj['position'],
            'timestamp': datetime.fromtimestamp(obj['timestamp'] / 1000).isoformat(),  # Convert from milliseconds to ISO string
            'race_id': obj.get('race_id')  # Include race_id if present
        }
    
    def _commentary_from_dict(self, obj, ctx):
        """Convert dictionary to Commentary object"""
        if obj is None:
            return None
        
        # Handle nullable fields from JSON Schema format
        commentary_id = obj.get('id')
        if commentary_id is None:
            return None
            
        message = obj.get('message')
        if message is None:
            return None
            
        timestamp = obj.get('timestamp')
        if timestamp is None:
            return None
            
        commentary_type = obj.get('type', 'info')
        if commentary_type is None:
            commentary_type = 'info'
        
        return Commentary(
            id=commentary_id,
            message=message,
            timestamp=datetime.fromtimestamp(timestamp / 1000),  # Convert from milliseconds
            type=commentary_type
        )
    
    def add_position_callback(self, callback):
        """Add callback for position updates"""
        self.position_callbacks.append(callback)
    
    def add_commentary_callback(self, callback):
        """Add callback for commentary updates"""
        self.commentary_callbacks.append(callback)
    
    def start_consuming(self):
        """Start consuming from Kafka topics"""
        if self.running:
            return
        
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        print(f"Started consuming from topics: {self.topics}")
    
    def stop_consuming(self):
        """Stop consuming from Kafka topics"""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join()
        self.consumer.close()
        print("Stopped consuming from Kafka")
    
    def _consume_loop(self):
        """Ultra-fast consumption loop for maximum real-time performance"""
        self.consumer.subscribe(self.topics)
        
        while self.running:
            try:
                # Ultra-aggressive polling with no timeout for maximum real-time performance
                msg_pack = self.consumer.consume(num_messages=1000, timeout=0)  # No timeout, process immediately
                
                if not msg_pack:
                    continue
                
                # Process messages immediately without any delays
                for msg in msg_pack:
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            print(f"Consumer error: {msg.error()}")
                            continue
                    
                    # Parse message using appropriate deserializer
                    topic = msg.topic()
                    
                    if topic == config.get_topic_names()['positions']:
                        # Deserialize positions using JSON_SR
                        data = self.positions_deserializer(
                            msg.value(),
                            SerializationContext(topic, MessageField.VALUE)
                        )
                        self._handle_position_update(data)
                    elif topic == config.get_topic_names()['commentary']:
                        # Deserialize commentary using JSON Schema
                        try:
                            data = self.commentary_deserializer(
                                msg.value(),
                                SerializationContext(topic, MessageField.VALUE)
                            )
                            if data is not None:
                                self._handle_commentary_update(data)
                        except Exception as deserialize_error:
                            print(f"Error deserializing commentary message: {deserialize_error}")
                            continue
                
            except Exception as e:
                print(f"Error consuming message: {e}")
                continue
    
    def _handle_position_update(self, data):
        """Handle driver position updates"""
        try:
            # Update the position for this specific driver
            driver_name = data['driver_name']
            position = data['position']
            race_id = data.get('race_id')
            
            # Handle race-based positions
            if race_id:
                if race_id not in self.race_positions:
                    self.race_positions[race_id] = []
                
                # Find and update existing position or add new one for this race
                updated = False
                for i, pos in enumerate(self.race_positions[race_id]):
                    if pos['driver_name'] == driver_name:
                        self.race_positions[race_id][i] = data
                        updated = True
                        break
                
                if not updated:
                    self.race_positions[race_id].append(data)
                
                # Keep only the latest 10 positions per race (one per driver)
                if len(self.race_positions[race_id]) > 10:
                    self.race_positions[race_id] = self.race_positions[race_id][-10:]
                
                # Notify callbacks with race context
                for callback in self.position_callbacks:
                    try:
                        callback(self.race_positions[race_id], race_id)
                    except Exception as e:
                        print(f"Error in position callback: {e}")
                
                print(f"Updated race {race_id} position: {driver_name}: P{position}")
            else:
                # Legacy handling for positions without race_id
                # Find and update existing position or add new one
                updated = False
                for i, pos in enumerate(self.latest_positions):
                    if pos['driver_name'] == driver_name:
                        self.latest_positions[i] = data
                        updated = True
                        break
                
                if not updated:
                    self.latest_positions.append(data)
                
                # Keep only the latest 10 positions (one per driver)
                if len(self.latest_positions) > 10:
                    self.latest_positions = self.latest_positions[-10:]
                
                # Notify callbacks
                for callback in self.position_callbacks:
                    try:
                        callback(self.latest_positions)
                    except Exception as e:
                        print(f"Error in position callback: {e}")
                
                print(f"Updated position: {driver_name}: P{position}")
            
        except Exception as e:
            print(f"Error handling position update: {e}")
    
    def _handle_commentary_update(self, data):
        """Handle commentary updates with maximum real-time performance"""
        try:
            # Add to latest commentary immediately
            self.latest_commentary.append(data)
            
            # Keep only last 50 commentary entries
            if len(self.latest_commentary) > 50:
                self.latest_commentary = self.latest_commentary[-50:]
            
            # Notify callbacks immediately for real-time processing
            for callback in self.commentary_callbacks:
                try:
                    callback(data)
                except Exception as e:
                    print(f"Error in commentary callback: {e}")
            
            # Log for debugging (can be removed for production)
            print(f"New commentary: {data.message}")
            
        except Exception as e:
            print(f"Error handling commentary update: {e}")
    
    def get_latest_positions(self, race_id=None):
        """Get latest driver positions"""
        if race_id:
            return self.race_positions.get(race_id, [])
        return self.latest_positions
    
    def get_latest_commentary(self):
        """Get latest commentary entries"""
        return self.latest_commentary

# Global consumer instance
kafka_consumer = KafkaConsumer()
