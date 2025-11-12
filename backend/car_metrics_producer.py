import asyncio
import json
import random
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from config import config
from models import F1_DRIVERS
from schemas import CAR_METRICS_SCHEMA

class CarMetricsProducer:
    def __init__(self):
        # Check if feature is enabled
        if not config.is_anomaly_detection_enabled():
            raise ValueError("Anomaly detection feature is not enabled in config.yaml")
        
        kafka_config = config.get_kafka_config()
        self.producer = Producer(kafka_config)
        topic_names = config.get_topic_names()
        self.topic = topic_names.get('car_metrics')
        if not self.topic:
            raise ValueError("car_metrics topic not found in config")
        
        self.drivers = F1_DRIVERS
        self.running = False
        self.active_races = {}  # race_id -> race_info
        
        # Setup Schema Registry
        schema_registry_config = config.get_schema_registry_config()
        self.schema_registry_client = SchemaRegistryClient(schema_registry_config)
        
        # Register schema with Schema Registry - MUST happen before creating serializer
        # The subject name must match what AvroSerializer will use: <topic-name>-value
        car_metrics_subject = f"{self.topic}-value"
        
        schema_str = json.dumps(CAR_METRICS_SCHEMA)
        
        try:
            from confluent_kafka.schema_registry import Schema
            from confluent_kafka.schema_registry.error import SchemaRegistryError
            
            schema = Schema(schema_str, schema_type="AVRO")
            
            # First, try to check if subject exists
            subject_exists = False
            try:
                self.schema_registry_client.get_latest_version(car_metrics_subject)
                subject_exists = True
                print(f"ℹ️  Subject '{car_metrics_subject}' already exists")
            except Exception:
                print(f"ℹ️  Subject '{car_metrics_subject}' does not exist - will create it")
            
            # Register the schema - this will create the subject if it doesn't exist
            try:
                schema_id = self.schema_registry_client.register_schema(
                    subject_name=car_metrics_subject,
                    schema=schema
                )
                if subject_exists:
                    print(f"✅ Updated car metrics schema with ID: {schema_id} for subject: {car_metrics_subject}")
                else:
                    print(f"✅ Created subject '{car_metrics_subject}' and registered schema with ID: {schema_id}")
                
                # Verify the registration worked
                try:
                    registered_schema = self.schema_registry_client.get_latest_version(car_metrics_subject)
                    print(f"   ✅ Verified: Subject '{car_metrics_subject}' exists with schema ID: {registered_schema.schema_id}")
                except Exception as verify_error:
                    print(f"   ⚠️  Could not verify schema registration: {verify_error}")
                    
            except SchemaRegistryError as e:
                error_str = str(e)
                error_code = getattr(e, 'status_code', None)
                
                # Check if schema already exists (409 Conflict or similar)
                if error_code == 409 or "409" in error_str or "already exists" in error_str.lower() or "conflict" in error_str.lower():
                    # Try to get the existing schema to verify it's correct
                    try:
                        latest_schema = self.schema_registry_client.get_latest_version(car_metrics_subject)
                        print(f"ℹ️  Car metrics schema already registered for subject: {car_metrics_subject} (schema ID: {latest_schema.schema_id})")
                        print(f"   Verifying schema compatibility...")
                        # Verify the schema matches
                        existing_schema_str = latest_schema.schema.schema_str
                        if existing_schema_str != schema_str:
                            print(f"⚠️  Warning: Existing schema differs from expected schema")
                            print(f"   This might cause deserialization issues. Consider updating the schema.")
                        else:
                            print(f"   ✅ Schema matches expected format")
                    except Exception as verify_error:
                        print(f"⚠️  Could not verify existing schema: {verify_error}")
                else:
                    # Other error - log it but continue
                    print(f"⚠️  Schema registration error: {e} (code: {error_code})")
                    print(f"   Will attempt to use schema on first message production")
                    raise  # Re-raise to ensure we know about the error
        except Exception as e:
            print(f"❌ Error during schema registration: {e}")
            print(f"   Schema subject: {car_metrics_subject}")
            print(f"   Topic: {self.topic}")
            print(f"   This is a critical error - schema must be registered before producing messages")
            raise  # Fail fast if schema registration fails
        
        # Setup Avro serializer for car metrics
        # The serializer will auto-register the schema if it's not already registered
        # But we prefer to register it explicitly above for better error handling
        try:
            self.avro_serializer = AvroSerializer(
                self.schema_registry_client,
                json.dumps(CAR_METRICS_SCHEMA),
                to_dict=self._car_metrics_to_dict
            )
            print(f"✅ Avro serializer created for car metrics")
        except Exception as e:
            print(f"❌ Failed to create Avro serializer: {e}")
            raise
        
        # Normal ranges for engine temperature
        self.engine_temp_range = {'min': 80.0, 'max': 100.0, 'normal': 90.0}
        
        # Track current engine temperature per team for gradual changes
        self.team_temperatures = {}
        for driver in self.drivers:
            if driver.team not in self.team_temperatures:
                self.team_temperatures[driver.team] = self.engine_temp_range['normal']
    
    def _car_metrics_to_dict(self, obj, ctx):
        """Convert car metrics object to dictionary for Avro serialization"""
        if obj is None:
            return None
        
        timestamp = obj.get('timestamp')
        if timestamp is None:
            timestamp = int(datetime.now().timestamp() * 1000)
        elif hasattr(timestamp, 'timestamp'):
            timestamp = int(timestamp.timestamp() * 1000)
        elif isinstance(timestamp, (int, float)):
            timestamp = int(timestamp)
        else:
            timestamp = int(datetime.now().timestamp() * 1000)
        
        return {
            "team_name": str(obj.get('team_name', '')),
            "timestamp": timestamp,
            "engine_temperature": float(obj.get('engine_temperature', 0.0))
        }
    
    def delivery_callback(self, err, msg):
        if err:
            print(f'Car metrics message delivery failed: {err}')
        else:
            print(f'Car metrics delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def _generate_engine_temperature(self, team_name):
        """Generate engine temperature with small random variation"""
        current = self.team_temperatures[team_name]
        
        # Small random variation around current value (±2-5%)
        variation = random.uniform(-0.05, 0.05) * current
        new_value = current + variation
        
        # Keep within normal bounds
        new_value = max(self.engine_temp_range['min'], min(self.engine_temp_range['max'], new_value))
        
        # Update stored value
        self.team_temperatures[team_name] = new_value
        
        return new_value
    
    def _inject_anomaly(self, team_name):
        """Inject an anomaly into engine temperature"""
        current = self.team_temperatures[team_name]
        anomaly_type = random.choice(['spike', 'drop'])
        
        if anomaly_type == 'spike':
            # Sudden spike (20-50% above normal max)
            anomaly_value = self.engine_temp_range['max'] * random.uniform(1.2, 1.5)
        else:  # drop
            # Sudden drop (20-50% below normal min)
            anomaly_value = self.engine_temp_range['min'] * random.uniform(0.5, 0.8)
        
        # Update stored value
        self.team_temperatures[team_name] = anomaly_value
        
        return anomaly_value
    
    def _generate_metrics_for_team(self, team_name):
        """Generate metrics for a team (simplified - only engine temperature)"""
        # Decide if we should inject an anomaly (5-10% probability)
        should_inject_anomaly = random.random() < random.uniform(0.05, 0.10)
        
        if should_inject_anomaly:
            engine_temp = self._inject_anomaly(team_name)
        else:
            engine_temp = self._generate_engine_temperature(team_name)
        
        return {
            'team_name': team_name,
            'timestamp': datetime.now(),
            'engine_temperature': engine_temp
        }
    
    async def produce_metrics_for_race(self, race_id: str, lap_number: int = 1):
        """Produce car metrics for all teams in a race"""
        if race_id not in self.active_races:
            return
        
        try:
            # Get unique teams from drivers
            teams = set(driver.team for driver in self.drivers)
            
            # Generate metrics for each team
            for team_name in teams:
                metrics_data = self._generate_metrics_for_team(team_name)
                
                # Serialize using Avro
                serialized_data = self.avro_serializer(
                    metrics_data,
                    SerializationContext(self.topic, MessageField.VALUE)
                )
                
                # Produce to Kafka with race_id as key
                self.producer.produce(
                    self.topic,
                    key=race_id,
                    value=serialized_data,
                    callback=self.delivery_callback
                )
            
            # Flush to ensure messages are sent
            self.producer.flush()
            
        except Exception as e:
            print(f"Error producing car metrics for race {race_id}: {e}")
    
    def start_race_metrics(self, race_id: str):
        """Start producing metrics for a race"""
        self.active_races[race_id] = {
            'start_time': datetime.now(),
            'lap_number': 1
        }
        print(f"Started car metrics production for race {race_id}")
    
    def stop_race_metrics(self, race_id: str):
        """Stop producing metrics for a race"""
        if race_id in self.active_races:
            del self.active_races[race_id]
            print(f"Stopped car metrics production for race {race_id}")
    
    async def produce_metrics_loop(self):
        """Main loop to continuously produce metrics for active races"""
        self.running = True
        print("Starting car metrics producer loop...")
        
        while self.running:
            try:
                for race_id in list(self.active_races.keys()):
                    race_info = self.active_races[race_id]
                    
                    # Calculate lap number based on elapsed time (roughly 1 lap per 10 seconds)
                    elapsed = (datetime.now() - race_info['start_time']).total_seconds()
                    lap_number = max(1, int(elapsed / 10) + 1)
                    race_info['lap_number'] = lap_number
                    
                    # Produce metrics for this race
                    await self.produce_metrics_for_race(race_id, lap_number)
                
                # Wait 0.5-1 second before next update
                await asyncio.sleep(random.uniform(0.5, 1.0))
                
            except Exception as e:
                print(f"Error in car metrics producer loop: {e}")
                await asyncio.sleep(1)
    
    def stop(self):
        """Stop the producer"""
        self.running = False
        self.producer.flush()
        print("Stopped car metrics producer")

# Global producer instance (will be None if feature is disabled)
car_metrics_producer = None

def get_car_metrics_producer():
    """Get or create the car metrics producer instance"""
    global car_metrics_producer
    if car_metrics_producer is None and config.is_anomaly_detection_enabled():
        try:
            car_metrics_producer = CarMetricsProducer()
        except Exception as e:
            print(f"Failed to initialize car metrics producer: {e}")
            return None
    return car_metrics_producer

