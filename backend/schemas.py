"""
Avro schemas for F1 Leaderboard application
These schemas define the data contracts for Kafka messages
"""

# Commentary Schema
COMMENTARY_SCHEMA = {
  "type": "object",
  "properties": {
    "id": {
      "type": ["string", "null"]
    },
    "message": {
      "type": ["string", "null"]
    },
    "timestamp": {
      "type": ["number", "null"]
    },
    "type": {
      "type": ["string", "null"]
    }
  },
  "required": ["id", "message", "timestamp", "type"]
}

# Leaderboard Update Schema (for positions topic) - Flat
LEADERBOARD_UPDATE_SCHEMA = {
    "type": "record",
    "name": "LeaderboardUpdate",
    "namespace": "com.f1leaderboard",
    "fields": [
        {
            "name": "driver_name",
            "type": "string",
            "doc": "Name of the driver"
        },
        {
            "name": "position",
            "type": "int",
            "doc": "Current position in the race (1-10)"
        },
        {
            "name": "timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Timestamp when the position was recorded"
        },
        {
            "name": "race_id",
            "type": ["null", "string"],
            "doc": "Unique identifier for the race"
        },
        {
            "name": "team_name",
            "type": ["null", "string"],
            "doc": "Name of the driver's team"
        },
        {
            "name": "speed",
            "type": ["null", "double"],
            "doc": "Current speed of the driver"
        }
    ]
}

# Car Metrics Schema (simplified - only essential fields)
CAR_METRICS_SCHEMA = {
    "type": "record",
    "name": "CarMetrics",
    "namespace": "com.f1leaderboard",
    "fields": [
        {
            "name": "team_name",
            "type": "string",
            "doc": "Name of the team"
        },
        {
            "name": "timestamp",
            "type": "long",
            "logicalType": "timestamp-millis",
            "doc": "Timestamp when the metrics were recorded"
        },
        {
            "name": "engine_temperature",
            "type": "double",
            "doc": "Engine temperature in Celsius"
        }
    ]
}

# Anomaly Value Schema (Avro format from Flink)
ANOMALY_VALUE_SCHEMA = {
    "type": "record",
    "name": "f1_car_metrics_anomalies_value",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {
            "default": None,
            "name": "team_name",
            "type": ["null", "string"]
        },
        {
            "default": None,
            "name": "engine_temperature",
            "type": ["null", "double"]
        },
        {
            "default": None,
            "name": "is_anomaly",
            "type": ["null", "boolean"]
        }
    ]
}

# Anomaly Key Schema (Avro format from Flink)
ANOMALY_KEY_SCHEMA = {
    "type": "record",
    "name": "car_metrics_anomalies_key",
    "namespace": "org.apache.flink.avro.generated.record",
    "fields": [
        {
            "name": "key",
            "type": "string"
        },
        {
            "name": "ts",
            "type": {
                "logicalType": "timestamp-millis",
                "type": "long"
            }
        }
    ]
}

