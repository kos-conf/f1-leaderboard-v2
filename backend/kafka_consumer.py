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
from schemas import LEADERBOARD_UPDATE_SCHEMA, COMMENTARY_SCHEMA, ANOMALY_VALUE_SCHEMA, ANOMALY_KEY_SCHEMA

class KafkaConsumer:
    def __init__(self):
        kafka_config = config.get_kafka_config()
        kafka_config['group.id'] = config.get_consumer_group()
        kafka_config['auto.offset.reset'] = 'latest'
        # Optimized consumer config for minimum latency
        kafka_config['fetch.min.bytes'] = 1  # Fetch messages as soon as available
        kafka_config['fetch.wait.max.ms'] = 10  # Minimal wait for low latency
        kafka_config['max.partition.fetch.bytes'] = 1048576  # 1MB per partition
        kafka_config['enable.auto.commit'] = True  # Auto-commit for speed
        kafka_config['queued.max.messages.kbytes'] = 51200  # 50MB queue for high throughput
        kafka_config['session.timeout.ms'] = 30000  # 30 second session timeout
        kafka_config['heartbeat.interval.ms'] = 10000  # 10 second heartbeat
        
        self.consumer = Consumer(kafka_config)
        topic_names = config.get_topic_names()
        
        # Build topics list - conditionally include anomaly topic
        self.topics = [topic_names['positions'], topic_names['commentary']]
        if config.is_anomaly_detection_enabled() and 'car_metrics_anomalies' in topic_names:
            self.topics.append(topic_names['car_metrics_anomalies'])
        
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
        
        # Use Avro deserializer for anomalies (from Flink with Avro format)
        if config.is_anomaly_detection_enabled() and 'car_metrics_anomalies' in topic_names:
            self.anomaly_value_deserializer = AvroDeserializer(
                self.schema_registry_client,
                json.dumps(ANOMALY_VALUE_SCHEMA),
                from_dict=self._anomaly_from_dict
            )
            self.anomaly_key_deserializer = AvroDeserializer(
                self.schema_registry_client,
                json.dumps(ANOMALY_KEY_SCHEMA),
                from_dict=None  # We don't need to process the key
            )
        else:
            self.anomaly_value_deserializer = None
            self.anomaly_key_deserializer = None
        
        # In-memory storage for latest data
        self.latest_positions = []  # Legacy - for backward compatibility
        self.race_positions = {}  # New - grouped by race_id
        self.latest_commentary = []
        self.latest_anomalies = []  # Store detected anomalies
        self.position_callbacks = []
        self.commentary_callbacks = []
        self.anomaly_callbacks = []
        
        # Counter for ensuring unique timestamps for anomalies
        self.anomaly_counter = 0
        
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
    
    def _anomaly_from_dict(self, obj, ctx):
        """Convert dictionary to anomaly data format"""
        if obj is None:
            return None
        
        # Extract fields from Avro deserialized object
        # The schema has: team_name, engine_temperature, is_anomaly
        return {
            'team_name': obj.get('team_name'),
            'engine_temperature': obj.get('engine_temperature'),
            'is_anomaly': obj.get('is_anomaly')
        }
    
    def add_position_callback(self, callback):
        """Add callback for position updates"""
        self.position_callbacks.append(callback)
    
    def add_commentary_callback(self, callback):
        """Add callback for commentary updates"""
        self.commentary_callbacks.append(callback)
    
    def add_anomaly_callback(self, callback):
        """Add callback for anomaly updates"""
        self.anomaly_callbacks.append(callback)
    
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
        """Optimized consumption loop for minimum latency - commentary prioritized"""
        self.consumer.subscribe(self.topics)
        
        while self.running:
            try:
                # Use small batch with minimal timeout for low latency
                # Process up to 5 messages at a time for efficiency
                msg_pack = self.consumer.consume(num_messages=5, timeout=0.01)
                
                if not msg_pack:
                    continue
                
                # Separate messages by type for prioritized processing
                commentary_messages = []
                position_messages = []
                anomaly_messages = []
                
                topic_names = config.get_topic_names()
                anomalies_topic = topic_names.get('car_metrics_anomalies')
                
                for msg in msg_pack:
                    if msg is None:
                        continue
                    
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            print(f"Consumer error: {msg.error()}")
                            continue
                    
                    topic = msg.topic()
                    if topic == topic_names['commentary']:
                        commentary_messages.append(msg)
                    elif topic == topic_names['positions']:
                        position_messages.append(msg)
                    elif anomalies_topic and topic == anomalies_topic:
                        anomaly_messages.append(msg)
                
                # Process commentary messages FIRST for immediate UI updates
                for msg in commentary_messages:
                    try:
                        data = self.commentary_deserializer(
                            msg.value(),
                            SerializationContext(msg.topic(), MessageField.VALUE)
                        )
                        if data is not None:
                            # Process commentary immediately - highest priority
                            self._handle_commentary_update(data)
                    except Exception as deserialize_error:
                        print(f"Error deserializing commentary message: {deserialize_error}")
                        continue
                
                # Then process position messages
                for msg in position_messages:
                    try:
                        data = self.positions_deserializer(
                            msg.value(),
                            SerializationContext(msg.topic(), MessageField.VALUE)
                        )
                        if data is not None:
                            self._handle_position_update(data)
                    except Exception as deserialize_error:
                        print(f"Error deserializing position message: {deserialize_error}")
                        continue
                
                # Process anomaly messages (from Flink, Avro format)
                for msg in anomaly_messages:
                    try:
                        if self.anomaly_value_deserializer:
                            data = self.anomaly_value_deserializer(
                                msg.value(),
                                SerializationContext(msg.topic(), MessageField.VALUE)
                            )
                            if data is not None:
                                self._handle_anomaly_update(data)
                    except Exception as deserialize_error:
                        print(f"Error deserializing anomaly message: {deserialize_error}")
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
        """Handle commentary updates with minimum latency - optimized for real-time"""
        try:
            # CRITICAL: Notify callbacks FIRST before any other operations
            # This ensures UI gets updates immediately without any delay
            for callback in self.commentary_callbacks:
                try:
                    callback(data)
                except Exception as e:
                    print(f"Error in commentary callback: {e}")
            
            # Update storage AFTER callbacks (non-blocking operation)
            # This doesn't affect UI latency
            self.latest_commentary.append(data)
            
            # Keep only last 50 commentary entries (do this less frequently)
            if len(self.latest_commentary) > 50:
                self.latest_commentary = self.latest_commentary[-50:]
            
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
    
    def _handle_anomaly_update(self, data):
        """Handle anomaly updates from Flink"""
        try:
            # Import here to avoid circular imports
            from models import F1_DRIVERS
            
            # Extract anomaly data (from Flink ML_DETECT_ANOMALIES output)
            # data is already a dict from _anomaly_from_dict
            team_name = data.get('team_name', '')
            
            # Derive driver_name from team_name (use first driver from team)
            driver_name = None
            for driver in F1_DRIVERS:
                if driver.team == team_name:
                    driver_name = driver.name
                    break
            
            # Use current time in milliseconds for timestamp
            # Add a counter to ensure uniqueness if multiple anomalies arrive at same millisecond
            import time
            self.anomaly_counter += 1
            timestamp = int(time.time() * 1000) + (self.anomaly_counter % 1000)  # Add counter for uniqueness
            
            anomaly_data = {
                'team_name': team_name,
                'driver_name': driver_name or team_name,  # Fallback to team_name if no driver found
                'timestamp': timestamp,
                'metric_name': 'Engine Temperature',
                'metric_value': data.get('engine_temperature') if data.get('engine_temperature') is not None else 0.0,
                'confidence_score': 0.95 if data.get('is_anomaly') else 0.0,  # Default confidence based on is_anomaly flag
                'is_anomaly': data.get('is_anomaly', False)
            }
            
            # Determine severity based on confidence and metric value
            anomaly_data['severity'] = self._determine_severity(anomaly_data)
            
            # Store in global list (no race_id in simplified schema)
            self.latest_anomalies.append(anomaly_data)
            # Keep only last 100 anomalies globally
            if len(self.latest_anomalies) > 100:
                self.latest_anomalies = self.latest_anomalies[-100:]
            
            # Notify callbacks
            for callback in self.anomaly_callbacks:
                try:
                    callback(anomaly_data)
                except Exception as e:
                    print(f"Error in anomaly callback: {e}")
            
            print(f"Anomaly detected: {anomaly_data['team_name']} - {anomaly_data['metric_name']}: {anomaly_data['metric_value']:.2f}°C")
            
        except Exception as e:
            print(f"Error handling anomaly update: {e}")
    
    def _determine_severity(self, anomaly_data):
        """Determine severity of anomaly based on confidence and value"""
        confidence = anomaly_data.get('confidence_score', 0.0)
        metric_name = anomaly_data.get('metric_name', '').lower()
        metric_value = anomaly_data.get('metric_value', 0.0)
        
        # Critical if high confidence and extreme values
        if confidence > 0.95:
            if 'temperature' in metric_name:
                if metric_value > 120 or metric_value < 60:
                    return 'critical'
            elif 'pressure' in metric_name:
                if metric_value > 2.5 or metric_value < 1.5:
                    return 'critical'
            return 'warning'
        elif confidence > 0.90:
            return 'warning'
        else:
            return 'info'
    
    def get_latest_anomalies(self, race_id=None):
        """Get latest anomalies (race_id parameter ignored in simplified schema)"""
        # Simplified schema doesn't include race_id, so return all anomalies
        return self.latest_anomalies

# Global consumer instance
kafka_consumer = KafkaConsumer()
