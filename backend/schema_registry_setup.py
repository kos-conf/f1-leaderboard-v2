"""
Script to register schemas with Confluent Schema Registry
Run this script to register the schemas before starting the application
"""

import json
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from config import config
from schemas import LEADERBOARD_UPDATE_SCHEMA, COMMENTARY_SCHEMA, CAR_METRICS_SCHEMA

def register_schemas():
    """Register schemas with Confluent Schema Registry"""
    schema_registry_config = config.get_schema_registry_config()
    client = SchemaRegistryClient(schema_registry_config)
    
    try:
        topic_names = config.get_topic_names()
        
        # Register LeaderboardUpdate schema
        positions_topic = topic_names.get('positions')
        positions_subject = f"{positions_topic}-value"
        positions_schema_str = json.dumps(LEADERBOARD_UPDATE_SCHEMA)
        
        print(f"Registering schema for subject: {positions_subject}")
        schema = Schema(positions_schema_str, schema_type="AVRO")
        schema_id = client.register_schema(
            subject_name=positions_subject,
            schema=schema
        )
        print(f"✅ Successfully registered positions schema with ID: {schema_id}")
        
        # Register Commentary schema
        commentary_topic = topic_names.get('commentary')
        commentary_subject = f"{commentary_topic}-value"
        commentary_schema_str = json.dumps(COMMENTARY_SCHEMA)
        
        print(f"Registering schema for subject: {commentary_subject}")
        schema = Schema(commentary_schema_str, schema_type="AVRO")
        schema_id = client.register_schema(
            subject_name=commentary_subject,
            schema=schema
        )
        print(f"✅ Successfully registered commentary schema with ID: {schema_id}")
        
        # Register Car Metrics schema if anomaly detection is enabled
        if config.is_anomaly_detection_enabled():
            car_metrics_topic = topic_names.get('car_metrics')
            if car_metrics_topic:
                car_metrics_subject = f"{car_metrics_topic}-value"
                car_metrics_schema_str = json.dumps(CAR_METRICS_SCHEMA)
                
                print(f"Registering schema for subject: {car_metrics_subject}")
                schema = Schema(car_metrics_schema_str, schema_type="AVRO")
                schema_id = client.register_schema(
                    subject_name=car_metrics_subject,
                    schema=schema
                )
                print(f"✅ Successfully registered car metrics schema with ID: {schema_id}")
            else:
                print("⚠️  Car metrics topic not found in config")
        else:
            print("ℹ️  Anomaly detection feature is disabled - skipping car metrics schema registration")
        
        print("\n🎉 All schemas registered successfully!")
        print("You can now start the application with: python main.py")
        
    except Exception as e:
        print(f"❌ Error registering schemas: {e}")
        print("\nMake sure your Schema Registry credentials are correct in config.yaml")
        print("and that the Schema Registry is accessible.")

if __name__ == "__main__":
    register_schemas()
