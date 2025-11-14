# F1 Real-Time Analytics Lab

## Building a Live F1 Leaderboard with Kafka and Flink

Build a real-time F1 racing leaderboard using Apache Kafka, Confluent Cloud, Apache Flink SQL, and React.

### What You'll Build
- Real-time race simulation with live position updates
- Performance analytics with average speed tracking
- Interactive driver selection and race management
- Live dashboard with Server-Sent Events

![](images/architecture.gif)

## Prerequisites

### Required Software
| Software | Version | Verification Command |
|----------|---------|---------------------|
| **Node.js** | 18+ | `node --version` |
| **npm** | 9+ | `npm --version` |
| **Python** | 3.11+ | `python3 --version` |
| **pip** | Latest | `pip3 --version` |
| **Git** | Latest | `git --version` |

### Required Accounts
- **Confluent Cloud Account** (Free tier available)
  - Sign up at: [https://www.confluent.io/confluent-cloud/tryfree](https://www.confluent.io/confluent-cloud/tryfree/)

### Pre-Lab Verification
```bash
node --version && npm --version
```
```bash
python3 --version && pip3 --version
```
```bash
git --version
```
```bash
python3 -m venv test_env && rm -rf test_env
```

## Part 1: Confluent Cloud Setup

### Step 1.1: Create Confluent Cloud Account
- Go to [https://www.confluent.io/confluent-cloud/tryfree](https://www.confluent.io/confluent-cloud/tryfree/)
- Sign up and verify your account

### Step 1.2: Get Confluent Cloud Management API Credentials

To deploy infrastructure programmatically, you need Confluent Cloud Management API credentials:

1. Go to [Create API Keys](https://confluent.cloud/settings/api-keys/create?tab=cloud)
2. Click **My account**
3. Choose **Cloud resource management** for resource scope and click next
4. Provide name and description (Optional) and click next
5. Download API key and click **Complete**

### Step 1.3: Update the Provided PROMOCODE using [this](https://confluent.cloud/settings/billing/payment) link

### Step 1.4: Configure Credentials

1. **Clone the repository:**
   ```bash
   git clone https://github.com/kos-conf/f1-leaderboard-v2.git
   ```
   ```bash
   cd f1-leaderboard-v2
   ```

2. **Create admin config file:**
   ```bash
   cd admin
   ```
   ```bash
   cp config.yaml.example config.yaml
   ```

3. **Edit `admin/config.yaml` and add your credentials:**
   ```yaml
   cloud_api_key: "your-api-key"
   cloud_api_secret: "your-api-secret"
   ```

4. **Return to root directory:**
   ```bash
   cd ..
   ```

## Part 2: Environment Setup

### Quick Setup (Recommended)

We provide automated setup scripts that handle all the setup steps for you:

**macOS/Linux:**
```bash
chmod +x setup.sh
```
```bash
./setup.sh
```

**Windows (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File setup.ps1
```

**Windows (Git Bash/WSL):**
```bash
./setup.sh
```

The setup script will:
- ✅ Check all prerequisites (Python, Node.js, npm)
- ✅ Set up backend virtual environment and install dependencies
- ✅ Set up admin virtual environment and install dependencies
- ✅ Set up frontend and install dependencies
- ✅ Deploy Confluent Cloud infrastructure (if credentials are configured)

> **Note:** Make sure you've configured `admin/config.yaml` with your Confluent Cloud API credentials (see Part 1) before running the setup script if you want to deploy infrastructure automatically.

<details>
<summary><b>Manual Setup (Alternative)</b></summary>

If you prefer to set up manually or the automated script doesn't work for your environment:

#### Step 1.1: Clone the Repository
```bash
git clone https://github.com/kos-conf/f1-leaderboard-v2.git && cd f1-leaderboard-v2
```

#### Step 1.2: Set Up Backend Environment

Create virtual environment:
```bash
cd backend && python3 -m venv venv
```

Activate virtual environment:

- On macOS/Linux:
  ```bash
  source venv/bin/activate
  ```

- On Windows: 
  ```bash
  venv\Scripts\activate
  ```

Install dependencies:
```bash
pip install -r requirements.txt
```

Return to root directory:
```bash
cd ..
```

#### Step 1.3: Set Up Admin Environment

```bash
cd admin && python3 -m venv venv
```

Activate virtual environment (same as above based on your OS), then:
```bash
pip install -r requirements.txt
```

Create config file:
```bash
cp config.yaml.example config.yaml
```

Return to root directory:
```bash
cd ..
```

#### Step 1.4: Set Up Frontend Environment
```bash
cd frontend
```
```bash
npm install
```
```bash
cd ..
```

</details>

<details>
<summary><b>Step 2.1: Deploy Infrastructure Using Admin Script (Manual Fallback)</b></summary>

> **Note:** The automated setup script (`./setup.sh` or `setup.ps1`) automatically deploys infrastructure if credentials are configured in `admin/config.yaml`. This step is only needed if:
> - The automated deployment failed and you need to retry
> - You prefer to deploy infrastructure manually
> - You want to re-run the deployment after making changes

1. **Activate virtual environment:**
   
   - On macOS/Linux:
     ```bash
     cd admin
     ```
     ```bash
     source venv/bin/activate
     ```
   
   - On Windows:
     ```bash
     cd admin
     ```
     ```powershell
     venv\Scripts\Activate.ps1
     ```

2. **Run the admin script:**
   ```bash
   python main.py
   ```

   This script will automatically:
   - Create a Confluent Cloud environment
   - Create a Basic Kafka cluster in AWS us-east-2 region
   - Create a Flink compute pool in AWS us-east-2 region
   - Create service account with appropriate roles
   - Create Kafka and Schema Registry API keys
   - Create the `f1-driver-positions` topic
   - Register Avro schemas (data contracts)
   - Update `backend/config.yaml` with credentials

   > **Note:** The script is idempotent - safe to run multiple times. It will skip existing resources.

3. **Verify configuration:**
   Check that `backend/config.yaml` has been updated with your credentials:
   ```yaml
   kafka:
     bootstrap.servers: 'your-cluster-endpoint'
     security.protocol: "SASL_SSL"
     sasl.mechanism: "PLAIN"
     sasl.username: 'your-kafka-api-key'
     sasl.password: 'your-kafka-api-secret'
     schema_registry_url: 'your-schema-registry-url'
     schema_registry_api_key: 'your-schema-registry-api-key'
     schema_registry_secret: 'your-schema-registry-secret'
     topics:
       positions: "f1-driver-positions"
       commentary: "f1-commentary"
     consumer_group: "f1-leaderboard-consumer"
   ```

4. **Return to root directory:**
   ```bash
   cd ..
   ```

</details>

## Part 3: Implement Flink SQL Analytics

### Step 3.1: Open SQL Workspace

1. **Navigate to Flink in Confluent Cloud:**
   - Go to [Flink UI](https://confluent.cloud/go/flink)
   - Select your environment from the dropdown

2. **Open SQL Workspace:**
   - Click on **Open SQL Workspace** button.

3. **Configure Catalog and Database:**
   - In the SQL Workspace, configure the catalog and database settings
   - Select your environment and Kafka cluster
   ![](images/catalog_database.png)

### Step 3.2: Realtime Analytics with Confluent Cloud for Apache Flink

Now that you have the SQL Workspace open, execute the following Flink SQL statements one by one:

1. **Create Bedrock Connection:**
   
   First, update the AWS credentials in the connection configuration:
   - Replace `'aws-access-key' = '***'` with your AWS access key
   - Replace `'aws-secret-key' = '***'` with your AWS secret key
   - Replace `'aws-session-token' = '***'` with your AWS session token (if using temporary credentials)

   Then execute:
   ```sql
   CREATE CONNECTION `bedrock-connection`
   WITH (
     'type' = 'BEDROCK',
     'endpoint' = 'https://bedrock-runtime.us-east-1.amazonaws.com/model/us.anthropic.claude-3-5-sonnet-20240620-v1:0/invoke',
     'aws-access-key' = '***',
     'aws-secret-key' = '***',
     'aws-session-token' = '***'
   );
   ```

2. **Create Commentary Generator Model:**
   ```sql
   CREATE MODEL f1_commentary_generator
   INPUT (race_context STRING)
   OUTPUT (commentary STRING)
   WITH (
     'bedrock.connection'='bedrock-connection',
     'provider'='bedrock',
     'task'='text_generation',
     'bedrock.params.max_tokens' = '20000'
   );
   ```
3. **Create f1-commentary topic:**
   ```sql
   CREATE TABLE `f1-commentary` (
    id STRING,
    message STRING,
    `timestamp` BIGINT,
    type STRING
   ) WITH (
         'value.format' = 'json-registry'
   );
   ```

4. **Generate Real-Time Commentary:**
   ```sql
   INSERT INTO `f1-commentary`
   SELECT 
       CONCAT('comment-', CAST(UNIX_TIMESTAMP() AS STRING), '-', REPLACE(driver_name, ' ', '_')) AS id,
       ai_result.commentary AS message,
       `timestamp`,
       CASE 
           WHEN `position` = 1 THEN 'highlight'
           WHEN `position` <= 3 THEN 'warning'
           ELSE 'info'
       END AS type
   FROM `f1-driver-positions`,
   LATERAL TABLE(AI_COMPLETE('f1_commentary_generator', 
       CONCAT('Driver: ', driver_name, 
              ', Position: ', CAST(`position` AS STRING), 
              '. Generate exciting F1 commentary. Keep it under 80 characters.')
   )) AS ai_result(commentary);
   ```

> **Note:** Make sure to replace the AWS credentials placeholders (`***`) with your actual AWS credentials before executing the first SQL statement.

## Part 4: Car Metrics and Anomaly Detection Setup (Optional)

This is an optional advanced feature that demonstrates real-time anomaly detection using Confluent Flink's ML_DETECT_ANOMALIES function. The feature is disabled by default and can be enabled via configuration.

### Step 4.1: Enable Anomaly Detection Feature

1. **Edit `backend/config.yaml`:**
   ```yaml
   features:
     anomaly_detection:
       enabled: true  # Set to true to enable anomaly detection
   ```

2. **Re-run the admin script** to create the required topics:
   
   **macOS/Linux:**
   ```bash
   cd admin
   ```
   ```bash
   source venv/bin/activate
   ```
   ```bash
   python main.py
   ```
   ```bash
   cd ..
   ```
   
   **Windows:**
   ```powershell
   cd admin
   ```
   ```powershell   
   venv\Scripts\Activate.ps1
   ```
   ```powershell
   python main.py
   ```
   ```powershell
   cd ..
   ```
   
   This will create:
   - `f1-car-metrics` topic (for car telemetry data)
   
   > **Note:** The `f1-car-metrics-anomalies` topic will be automatically created by Flink when you execute the anomaly detection INSERT statement (see Step 4.2).
   
### Step 4.2: Set Up Flink SQL for Anomaly Detection

1. **Open SQL Workspace** in Confluent Cloud Flink (same as Part 3)

2. **Create sink table for detected anomalies (Flink will auto-create the Kafka topic):**
   ```sql
   CREATE TABLE `f1-car-metrics-anomalies` (
    key STRING,
    ts TIMESTAMP_LTZ(3),
    team_name STRING,
    engine_temperature DOUBLE,
    is_anomaly BOOLEAN,
    PRIMARY KEY (key, ts) NOT ENFORCED
   )
   DISTRIBUTED BY (key, ts)
   WITH (
      'changelog.mode' = 'upsert'
   );
   ```
   
   > **Note:** Using `changelog.mode = 'append'` because `ML_DETECT_ANOMALIES` in window functions doesn't support retraction. Anomalies are append-only events.
   
   > **Note:** The `f1-car-metrics-anomalies` Kafka topic will be automatically created by Flink when you execute the INSERT statement below.

3. **Create simple Flink SQL query for anomaly detection:**
   ```sql
    SET 'sql.state-ttl' = '24 h';
    INSERT INTO `f1-car-metrics-anomalies`
    SELECT
      CAST(key AS STRING) AS key,
      TO_TIMESTAMP_LTZ(`timestamp`, 3) AS ts,
      team_name,
      engine_temperature,
      s.anomaly_results[6] AS is_anomaly
    FROM (
      SELECT
        key,
        team_name,
        `timestamp`,
        engine_temperature,
        ML_DETECT_ANOMALIES(
          engine_temperature,
          TO_TIMESTAMP_LTZ(`timestamp`, 3),
          JSON_OBJECT(
            'horizon' VALUE 1,
            'confidencePercentage' VALUE 90.0,
            'minTrainingSize' VALUE 16
          )
        ) OVER (
          PARTITION BY team_name
          ORDER BY TO_TIMESTAMP_LTZ(`timestamp`, 3)
          RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS anomaly_results
      FROM `f1-car-metrics`
    ) AS s
    WHERE s.anomaly_results[6] = TRUE;
   ```

## Part 5: Running the Application

Now that all Flink SQL statements (including optional anomaly detection) are set up, start the backend and frontend servers:

### Step 5.1: Start the Backend Server

**macOS/Linux:**
```bash
cd backend
```
```bash
source venv/bin/activate
```
```bash
python3 main.py
```

**Windows:**
```powershell
cd backend
```
```powershell
venv\Scripts\Activate.ps1
```
```powershell
python main.py
```

> **Note: This command needs to be running all the time. Do not stop this server. Please continue the lab on a new Terminal Tab.**

### Step 5.2: Start the Frontend Application
Open a new terminal. Make sure you are in the directory of the repository. Then run the following.
```bash
cd frontend
```
```bash
npm run dev
```

> **Note: This command needs to be running all the time. Do not stop this server. Please continue the lab on a new Terminal Tab.**

### Step 5.3: View Anomalies in UI

Once the feature is enabled and Flink queries are running:

1. **Start a race** from the (frontend)[http://localhost:5173] application
2. **Click on the View Anomalies button** on the right bottom of the screen at anytime
3. **Anomalies will appear in real-time** as they are detected by Flink

### Feature Behavior

- **When enabled**: Car metrics are continuously produced during races, Flink detects anomalies, and they appear in the UI
- **When disabled**: No car metrics are produced, no Flink queries needed, and the UI shows a message that the feature is disabled
- **Default**: Feature is disabled (`enabled: false`) for backward compatibility

> **Note:** The anomaly detection feature requires additional Flink compute resources. Make sure your Confluent Cloud account has sufficient capacity.

<details>
<summary><b>Part 7: Vector Search Against Vector Database (Optional)</b></summary>

This is an optional advanced feature that demonstrates how to perform vector search operations against a MongoDB vector database using Confluent Cloud for Apache Flink. You'll learn how to create embeddings from search queries and perform similarity searches.

### Step 7.1: Create MongoDB Connection

1. **Navigate to Connections in Confluent Cloud:**
   - Go to [Manage Connections](https://docs.confluent.io/cloud/current/integrations/connections/manage-connections.html#create-a-connection)
   - Click **Create Connection**
   - Select **MongoDB** as the connection type
   - Configure your MongoDB connection details (host, port, database, credentials)
   - Save the connection and note the connection name (e.g., `mongodb-connection-23eb1bdb-a806-4db3-a50e-820271af1a89`)

   > **Note:** Replace the connection name in the SQL statements below with your actual MongoDB connection name.

### Step 7.2: Create OpenAI Embedding Connection

1. **Open SQL Workspace** in Confluent Cloud Flink (same as Part 3)

2. **Create OpenAI connection for embeddings:**
   
   First, update the API key in the connection configuration:
   - Replace `'api-key' = '*****'` with your OpenAI API key

   Then execute:
   ```sql
   CREATE CONNECTION openai_embedding_connection
   WITH (
     'type' = 'openai',
     'endpoint' = 'https://api.openai.com/v1/embeddings',
     'api-key' = '*****'
   );
   ```

   > **Note:** Make sure to replace the API key placeholder (`*****`) with your actual OpenAI API key before executing.

### Step 7.3: Create Embedding Model

Create a model to generate embeddings from text input:

```sql
CREATE MODEL plot_embed
INPUT (input STRING)
OUTPUT (embedding ARRAY<FLOAT>)
WITH (
  'provider' = 'openai',
  'task' = 'embedding',
  'openai.connection' = 'openai_embedding_connection'
);
```

### Step 7.4: Create External MongoDB Table

Create an external table that references your MongoDB collection with vector embeddings:

```sql
CREATE TABLE mongodb_movies (
  title STRING,
  plot STRING,
  plot_embedding ARRAY<FLOAT>
) WITH (
  'connector' = 'mongodb',
  'mongodb.connection' = 'mongodb-connection-23eb1bdb-a806-4db3-a50e-820271af1a89',
  'mongodb.database' = 'sample_mflix',
  'mongodb.collection' = 'embedded_movies',
  'mongodb.index' = 'vector_index',
  'mongodb.numcandidates' = '100'
);
```

> **Note:** Replace `'mongodb-connection-23eb1bdb-a806-4db3-a50e-820271af1a89'` with your actual MongoDB connection name from Step 7.1. Also ensure your MongoDB collection has a vector index configured.

### Step 7.5: Create Search Query Tables

Create tables to store search queries and their vector embeddings:

1. **Create table for search queries:**
   ```sql
   CREATE TABLE movie_plot_queries (plot STRING);
   ```

2. **Insert sample search query:**
   ```sql
   INSERT INTO movie_plot_queries VALUES
   ('A major countermands orders and attacks to revenge a previous massacre of men, women and children.');
   ```

3. **Create table to store query vectors:**
   ```sql
   CREATE TABLE movie_plot_vectors (plot STRING, vector ARRAY<FLOAT>);
   ```

### Step 7.6: Generate Embeddings for Search Queries

Generate vector embeddings for the search queries using the embedding model:

```sql
INSERT INTO movie_plot_vectors
SELECT plot, embedding
FROM movie_plot_queries,
LATERAL TABLE(ML_PREDICT('plot_embed', plot));
```

### Step 7.7: Create Result Table and Perform Vector Search

1. **Create table to store vector search results:**
   ```sql
   CREATE TABLE movies_vector_search_result (
     plot STRING,
     search_results ARRAY<ROW<title STRING, plot STRING, plot_embedding ARRAY<FLOAT>, score DOUBLE>>
   );
   ```

2. **Perform vector search operation:**
   ```sql
   INSERT INTO movies_vector_search_result
   SELECT *
   FROM movie_plot_vectors q,
   LATERAL TABLE(
     VECTOR_SEARCH_AGG(mongodb_movies, DESCRIPTOR(plot_embedding), q.vector, 5)
   );
   ```

   This query performs a vector similarity search, finding the top 5 most similar movie plots in MongoDB based on the query embedding.

### Step 7.8: View Search Results

Query the results to see the matched movies with their similarity scores:

```sql
SELECT
  mv.plot AS query_plot,
  r.plot,
  r.title,
  r.score
FROM movies_vector_search_result AS mv
CROSS JOIN UNNEST(search_results) AS r(title, plot, plot_embedding, score);
```

This will display:
- The original search query plot
- Matched movie titles from MongoDB
- Matched movie plots
- Similarity scores (higher scores indicate better matches)

> **Note:** The vector search uses cosine similarity to find the most relevant matches. Ensure your MongoDB collection has documents with pre-computed `plot_embedding` vectors and a vector index configured for optimal performance.

</details>

## Results
![](images/finished.png)

## Cleanup

### Stop the Application
- Stop backend: `Ctrl+C` in backend terminal
- Stop frontend: `Ctrl+C` in frontend terminal

### Clean Up Confluent Cloud

Before running the teardown script, stop all Flink SQL statements:

1. **Go to Flink SQL Workspace:**
   - Navigate to your Confluent Cloud console
   - Go to the Flink SQL Workspace
   - Stop all running statements

Use the teardown script to automatically delete all resources:

1. **Navigate to admin directory:**
   ```bash
   cd admin
   ```

2. **Activate virtual environment (if not already activated):**
   
   **macOS/Linux:**
   ```bash
   source venv/bin/activate
   ```
   
   **Windows:**
   ```powershell
   venv\Scripts\Activate.ps1
   ```

3. **Run the teardown script:**
   ```bash
   python teardown.py
   ```

   This script will automatically:
   - Delete Schema Registry subjects
   - Delete Kafka topics
   - Delete API keys (Kafka and Schema Registry)
   - Delete Flink compute pool
   - Delete Kafka cluster
   - Delete service account
   - Delete environment

   > **Note:** The script uses the same configuration as `main.py` (environment variables or `admin/config.yaml`). Make sure you have your Confluent Cloud Management API credentials configured.

4. **Verify deletion:**
   - Check the Confluent Cloud console to confirm all resources have been deleted
   - Some resources may take a few minutes to fully delete

### Local Cleanup
```bash
rm -rf backend/venv
rm -rf frontend/node_modules
rm -rf admin/venv
```

## Resources
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/current/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Apache Flink Documentation](https://flink.apache.org/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [React Documentation](https://react.dev/)

---

**🎉 Lab Complete!** You've successfully built a real-time F1 analytics application.

