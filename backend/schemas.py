"""
Avro schemas for F1 Leaderboard application
These schemas define the data contracts for Kafka messages
"""

# Driver Position Schema (Flat)
DRIVER_POSITION_SCHEMA = {
    "type": "record",
    "name": "DriverPosition",
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
        }
    ]
}

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
            "default": None,
            "doc": "Unique identifier for the race"
        }
    ]
}

# Schema Registry subject names
SCHEMA_SUBJECTS = {
    "positions": "f1-driver-positions2-value",
    "commentary": "f1-commentary-value"
}
