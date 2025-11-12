# Confluent Admin Infrastructure Setup

This directory contains scripts to programmatically create the Confluent Cloud infrastructure required for the F1 Leaderboard application.

## What It Creates

The `main.py` script creates the following infrastructure:

1. **Confluent Cloud Environment** - A new environment to organize resources
2. **Kafka Cluster (Basic)** - A Basic Kafka cluster in AWS us-east-2 region
3. **Flink Compute Pool** - A Flink compute pool in AWS us-east-2 region for AI-powered commentary
4. **Kafka Topic**:
   - `f1-driver-positions` - For driver position updates
5. **Schema Registry Subject**:
   - `f1-driver-positions-value` - Avro schema for position updates

## Prerequisites

1. **Confluent Cloud Account** - You need a Confluent Cloud account
2. **Confluent Cloud Management API Credentials** - API key and secret with permissions to:
   - Create environments
   - Create Kafka clusters
   - Create Flink compute pools
3. **Python 3.11+** - Python 3.11 or higher
4. **Dependencies** - Install required packages (see Installation)

## Installation

1. **Create and activate a Python virtual environment:**
   ```bash
   cd admin
   ```
   ```bash
   python3 -m venv venv
   ```
   ```bash
   source venv/bin/activate
   ```

2. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up credentials:**
   
   Option 1: Environment variables
   ```bash
   export CONFLUENT_CLOUD_API_KEY="your-api-key"
   export CONFLUENT_CLOUD_API_SECRET="your-api-secret"
   ```
   
   Option 2: Configuration file. Edit config.yaml and add your credentials
   ```bash
   cp config.yaml.example config.yaml
   ```

## Usage

### Basic Usage

Run the script from the admin directory (make sure your virtual environment is activated):

```bash
cd admin
```
```bash
source venv/bin/activate
```
```bash
python main.py
```

### Configuration Options

You can configure the script using:

1. **Environment Variables:**
   - `CONFLUENT_CLOUD_API_KEY` - Cloud Management API key
   - `CONFLUENT_CLOUD_API_SECRET` - Cloud Management API secret
   - `CONFLUENT_ENVIRONMENT_NAME` - Environment name (default: `f1-leaderboard-env`)
   - `CONFLUENT_KAFKA_CLUSTER_NAME` - Kafka cluster name (default: `f1-leaderboard-cluster`)
   - `CONFLUENT_FLINK_POOL_NAME` - Flink compute pool name (default: `f1-leaderboard-flink-pool`)
   - `CONFLUENT_CLOUD_PROVIDER` - Cloud provider (default: `aws`)
   - `CONFLUENT_REGION` - Region (default: `us-east-2`)
   - `CONFLUENT_FLINK_MAX_CFU` - Max CFU for Flink pool (default: `5`)

2. **Configuration File (`admin/config.yaml`):**
   ```yaml
   cloud_api_key: "your-api-key"
   cloud_api_secret: "your-api-secret"
   environment_name: "f1-leaderboard-env"
   kafka_cluster_name: "f1-leaderboard-cluster"
   flink_pool_name: "f1-leaderboard-flink-pool"
   cloud_provider: "aws"
   region: "us-east-2"
   flink_max_cfu: 5
   ```

## Workflow

1. **First Run** - Creates environment, cluster, and Flink pool:
   ```bash
   python main.py
   ```
   This will create the cloud resources but may skip topic/schema creation if Kafka credentials aren't set up yet.

2. **After Creating API Keys** - Once you have:
   - Kafka cluster API keys
   - Schema Registry API keys
   
   Update `backend/config.yaml` with these credentials, then run again:
   ```bash
   python main.py
   ```
   This will create the topics and register schemas.

## Getting API Keys

### Confluent Cloud Management API Keys

1. Go to [Create API Keys](https://confluent.cloud/settings/api-keys/create?tab=cloud)
2. Click **My account**
3. Choose **Cloud resource management** for resource scope and click next.
4. Provide name and description(Optional) and click next
5. Download API key and click **Complete**

### Kafka Cluster API Keys

1. Go to your Kafka cluster in Confluent Cloud
2. Navigate to **API Keys** in the cluster settings
3. Click **Add API Key**
4. Select the cluster and environment
5. Copy the key and secret
6. Add to `backend/config.yaml`:
   ```yaml
   kafka:
     sasl.username: "your-kafka-api-key"
     sasl.password: "your-kafka-api-secret"
     bootstrap.servers: "your-cluster-endpoint"
   ```

### Schema Registry API Keys

1. Go to **Schema Registry** in Confluent Cloud
2. Navigate to **API Keys**
3. Click **Add API Key**
4. Copy the key and secret
5. Add to `backend/config.yaml`:
   ```yaml
   kafka:
     schema_registry_url: "https://your-schema-registry-url"
     schema_registry_api_key: "your-schema-registry-key"
     schema_registry_secret: "your-schema-registry-secret"
   ```

## Error Handling

The script handles common scenarios:

- **Existing Resources**: If an environment, cluster, or pool already exists, it will find and use the existing one
- **Existing Topics**: If topics already exist, it will skip creation with a warning
- **Existing Schemas**: If schemas already exist, it will skip registration with a warning
- **Missing Credentials**: Clear error messages guide you to set up credentials

## Troubleshooting

### "Failed to create Kafka cluster"
- Check that your API key has permissions to create clusters
- Verify the region is available in your account
- Ensure you have available cluster capacity

### "Failed to create topics"
- Make sure Kafka API keys are set in `backend/config.yaml`
- Verify the cluster is in "UP" status (may take a few minutes)
- Check that the bootstrap servers URL is correct

### "Failed to register schemas"
- Ensure Schema Registry API keys are set in `backend/config.yaml`
- Verify Schema Registry is enabled for your cluster
- Check that the Schema Registry URL is correct

## Notes

- The script waits for the Kafka cluster to be ready before proceeding
- Basic clusters are created by default (single zone, 1 partition per topic)
- All resources are created in the same environment for organization
- The script is idempotent - safe to run multiple times

