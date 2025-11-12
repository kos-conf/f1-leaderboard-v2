"""
Confluent Admin Infrastructure Teardown Script

This script deletes all Confluent Cloud infrastructure created by main.py:
- Schema Registry Subjects
- Kafka Topics
- Flink Compute Pool
- Kafka Cluster
- Service Account
- Environment
"""

import os
import sys
import time
import base64
import requests
from typing import Dict, Any, Optional
from pathlib import Path

# Add parent directory to path to import backend modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import KafkaException

# Import schemas from backend
try:
    from backend.schemas import SCHEMA_SUBJECTS
except ImportError:
    # Fallback if running from admin directory
    sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))


class ConfluentCloudManager:
    """Manages Confluent Cloud resources via Management API"""
    
    BASE_URL = "https://api.confluent.cloud"
    
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.auth_header = self._get_auth_header()
        self.headers = {
            'Authorization': f'Basic {self.auth_header}',
            'Content-Type': 'application/json'
        }
    
    def _get_auth_header(self) -> str:
        """Create Basic Auth header from API key and secret"""
        credentials = f"{self.api_key}:{self.api_secret}"
        return base64.b64encode(credentials.encode()).decode()
    
    def _find_environment_by_name(self, name: str) -> Optional[str]:
        """Find environment by name"""
        url = f"{self.BASE_URL}/org/v2/environments"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                environments = response.json().get('data', [])
                for env in environments:
                    if env.get('display_name') == name:
                        return env['id']
        except Exception as e:
            print(f"⚠️  Error searching for environment: {e}")
        return None
    
    def _find_cluster_by_name(self, env_id: str, name: str) -> Optional[str]:
        """Find Kafka cluster by name in environment"""
        url = f"{self.BASE_URL}/cmk/v2/clusters"
        params = {"environment": env_id}
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                clusters = data.get('data', []) if data else []
                if not isinstance(clusters, list):
                    clusters = []
                for cluster in clusters:
                    if cluster.get('spec', {}).get('display_name') == name:
                        return cluster['id']
        except Exception as e:
            print(f"⚠️  Error searching for cluster: {e}")
        return None
    
    def _find_flink_pool_by_name(self, env_id: str, name: str) -> Optional[str]:
        """Find Flink compute pool by name in environment"""
        url = f"{self.BASE_URL}/fcpm/v2/compute-pools"
        params = {"environment": env_id}
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                pools = data.get('data', []) if data else []
                if not isinstance(pools, list):
                    pools = []
                for pool in pools:
                    if pool.get('spec', {}).get('display_name') == name:
                        return pool['id']
        except Exception as e:
            print(f"⚠️  Error searching for Flink compute pool: {e}")
        return None
    
    def _find_service_account_by_name(self, name: str) -> Optional[str]:
        """Find a service account by name"""
        url = f"{self.BASE_URL}/iam/v2/service-accounts"
        try:
            page_size = 100
            page_token = None
            
            while True:
                params = {"page_size": page_size}
                if page_token:
                    params["page_token"] = page_token
                
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    service_accounts = data.get('data', [])
                    
                    for sa in service_accounts:
                        sa_name = (
                            sa.get('display_name') or 
                            sa.get('spec', {}).get('display_name') or
                            sa.get('name') or
                            sa.get('spec', {}).get('name')
                        )
                        if sa_name == name:
                            sa_id = sa.get('id')
                            if sa_id:
                                return sa_id
                    
                    metadata = data.get('metadata', {})
                    next_page_token = metadata.get('next', {}).get('page_token')
                    if not next_page_token:
                        break
                    page_token = next_page_token
                else:
                    break
        except Exception as e:
            print(f"⚠️  Error searching for service account: {e}")
        return None
    
    def list_api_keys_for_service_account(self, service_account_id: str, service_account_name: str = None) -> list:
        """List all API keys owned by a service account"""
        url = f"{self.BASE_URL}/iam/v2/api-keys"
        api_keys = []
        
        try:
            # Get all API keys and filter by service account
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                all_keys = data.get('data', [])
                
                # Filter keys that belong to the service account
                for key in all_keys:
                    owner = key.get('spec', {}).get('owner', {})
                    
                    # Check if owner ID matches service account ID
                    owner_id = owner.get('id')
                    owner_kind = owner.get('kind', '')
                    owner_name = owner.get('name', '')
                    
                    # Match by ID and kind (primary check)
                    id_matches = (
                        owner_id == service_account_id or
                        owner_id == f"sa-{service_account_id}" or
                        owner_id == f"User:{service_account_id}"
                    )
                    
                    kind_matches = owner_kind in ['ServiceAccount', 'User']
                    
                    # Match by name if provided (secondary check)
                    name_matches = True
                    if service_account_name:
                        name_matches = owner_name == service_account_name
                    
                    # Include key if ID and kind match, and optionally name matches
                    if id_matches and kind_matches and name_matches:
                        api_keys.append(key)
        except Exception as e:
            print(f"⚠️  Error listing API keys: {e}")
        
        return api_keys
    
    def delete_api_key(self, api_key_id: str) -> bool:
        """Delete an API key"""
        url = f"{self.BASE_URL}/iam/v2/api-keys/{api_key_id}"
        try:
            response = requests.delete(url, headers=self.headers)
            if response.status_code in [200, 204]:
                print(f"   ✅ Deleted API key: {api_key_id}")
                return True
            else:
                print(f"   ⚠️  Failed to delete API key {api_key_id}: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error deleting API key: {e}")
            return False
    
    def delete_flink_compute_pool(self, env_id: str, pool_id: str) -> bool:
        """Delete a Flink compute pool"""
        url = f"{self.BASE_URL}/fcpm/v2/compute-pools/{pool_id}"
        params = {"environment": env_id}
        
        try:
            response = requests.delete(url, headers=self.headers, params=params)
            if response.status_code in [200, 202, 204]:
                print(f"   ✅ Flink compute pool deletion initiated: {pool_id}")
                # Wait for deletion to complete
                self._wait_for_flink_pool_deletion(env_id, pool_id)
                return True
            else:
                print(f"   ⚠️  Failed to delete Flink compute pool: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error deleting Flink compute pool: {e}")
            return False
    
    def _wait_for_flink_pool_deletion(self, env_id: str, pool_id: str, max_wait: int = 300):
        """Wait for Flink compute pool to be deleted"""
        url = f"{self.BASE_URL}/fcpm/v2/compute-pools/{pool_id}"
        params = {"environment": env_id}
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 404:
                    print(f"   ✅ Flink compute pool deleted successfully")
                    return True
                elif response.status_code == 200:
                    status = response.json().get('status', {}).get('phase', '')
                    if status in ['DELETING']:
                        print(f"   ⏳ Flink compute pool deletion in progress...")
                        time.sleep(10)
                    else:
                        time.sleep(5)
                else:
                    time.sleep(5)
            except Exception as e:
                # 404 means it's deleted
                if "404" in str(e):
                    print(f"   ✅ Flink compute pool deleted successfully")
                    return True
                time.sleep(5)
        
        print(f"   ⚠️  Flink compute pool deletion may still be in progress")
        return False
    
    def delete_kafka_cluster(self, env_id: str, cluster_id: str) -> bool:
        """Delete a Kafka cluster"""
        url = f"{self.BASE_URL}/cmk/v2/clusters/{cluster_id}"
        params = {"environment": env_id}
        
        try:
            response = requests.delete(url, headers=self.headers, params=params)
            if response.status_code in [200, 202, 204]:
                print(f"   ✅ Kafka cluster deletion initiated: {cluster_id}")
                # Wait for deletion to complete
                self._wait_for_cluster_deletion(env_id, cluster_id)
                return True
            else:
                print(f"   ⚠️  Failed to delete Kafka cluster: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error deleting Kafka cluster: {e}")
            return False
    
    def _wait_for_cluster_deletion(self, env_id: str, cluster_id: str, max_wait: int = 600):
        """Wait for Kafka cluster to be deleted"""
        url = f"{self.BASE_URL}/cmk/v2/clusters/{cluster_id}"
        params = {"environment": env_id}
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 404:
                    print(f"   ✅ Kafka cluster deleted successfully")
                    return True
                elif response.status_code == 200:
                    status = response.json().get('status', {}).get('phase', '')
                    if status in ['DELETING']:
                        print(f"   ⏳ Kafka cluster deletion in progress...")
                        time.sleep(15)
                    else:
                        time.sleep(10)
                else:
                    time.sleep(10)
            except Exception as e:
                # 404 means it's deleted
                if "404" in str(e):
                    print(f"   ✅ Kafka cluster deleted successfully")
                    return True
                time.sleep(10)
        
        print(f"   ⚠️  Kafka cluster deletion may still be in progress")
        return False
    
    def delete_service_account(self, service_account_id: str) -> bool:
        """Delete a service account"""
        url = f"{self.BASE_URL}/iam/v2/service-accounts/{service_account_id}"
        
        try:
            response = requests.delete(url, headers=self.headers)
            if response.status_code in [200, 204]:
                print(f"   ✅ Deleted service account: {service_account_id}")
                return True
            else:
                print(f"   ⚠️  Failed to delete service account: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error deleting service account: {e}")
            return False
    
    def delete_environment(self, env_id: str) -> bool:
        """Delete an environment"""
        url = f"{self.BASE_URL}/org/v2/environments/{env_id}"
        
        try:
            response = requests.delete(url, headers=self.headers)
            if response.status_code in [200, 204]:
                print(f"   ✅ Environment deletion initiated: {env_id}")
                return True
            else:
                print(f"   ⚠️  Failed to delete environment: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"   ⚠️  Error deleting environment: {e}")
            return False
    
    def get_schema_registry_info(self, env_id: str, cluster_id: str = None) -> tuple:
        """Get the Schema Registry cluster ID and endpoint URL from the environment"""
        url = f"{self.BASE_URL}/srcm/v3/clusters"
        params = {"environment": env_id}
        sr_cluster_id = None
        sr_endpoint = None
        
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            items = data.get('data', [])
            
            if items:
                item = items[0]
                sr_cluster_id = item.get('id')
                
                spec = item.get('spec', {})
                status = item.get('status', {})
                
                sr_endpoint = spec.get('http', {}).get('endpoint')
                
                if not sr_endpoint:
                    sr_endpoint = (
                        spec.get('endpoint') or
                        spec.get('http_endpoint') or
                        status.get('http', {}).get('endpoint') or
                        status.get('endpoint') or
                        status.get('http_endpoint') or
                        item.get('endpoint') or
                        item.get('http_endpoint')
                    )
        except Exception as e:
            pass
        
        return sr_cluster_id, sr_endpoint


class KafkaAdmin:
    """Manages Kafka topics using AdminClient"""
    
    def __init__(self, kafka_config: Dict[str, Any]):
        self.admin_client = AdminClient(kafka_config)
        self.kafka_config = kafka_config
    
    def topic_exists(self, topic_name: str) -> bool:
        """Check if a topic exists"""
        try:
            metadata = self.admin_client.list_topics(timeout=10)
            topics = metadata.topics
            return topic_name in topics
        except Exception as e:
            print(f"   ⚠️  Error checking if topic '{topic_name}' exists: {e}")
            return False
    
    def delete_topics(self, topic_names: list) -> bool:
        """Delete Kafka topics"""
        futures = self.admin_client.delete_topics(topic_names, operation_timeout=60)
        
        success = True
        for topic, future in futures.items():
            try:
                future.result(timeout=60)
                print(f"   ✅ Topic '{topic}' deleted successfully")
            except KafkaException as e:
                if "does not exist" in str(e).lower() or "UnknownTopicOrPartitionException" in str(e):
                    print(f"   ⚠️  Topic '{topic}' does not exist")
                else:
                    print(f"   ⚠️  Failed to delete topic '{topic}': {e}")
                    success = False
            except Exception as e:
                print(f"   ⚠️  Error deleting topic '{topic}': {e}")
                success = False
        
        return success


class SchemaRegistryAdmin:
    """Manages Schema Registry subjects"""
    
    def __init__(self, schema_registry_config: Dict[str, str]):
        self.client = SchemaRegistryClient(schema_registry_config)
        self.config = schema_registry_config
    
    def subject_exists(self, subject: str) -> bool:
        """Check if a schema subject exists"""
        try:
            versions = self.client.get_versions(subject)
            return len(versions) > 0
        except Exception as e:
            error_str = str(e).lower()
            if "does not exist" in error_str or "not found" in error_str or "404" in error_str:
                return False
            # If it's an auth error, we can't determine existence, return False to skip
            if "401" in str(e) or "unauthorized" in error_str:
                return False
            return False
    
    def delete_subject(self, subject: str, permanent: bool = False) -> bool:
        """Delete a schema subject from Schema Registry"""
        try:
            if permanent:
                # Delete permanently (hard delete)
                versions = self.client.get_versions(subject)
                for version in versions:
                    self.client.delete_schema_version(subject, version, permanent=True)
                self.client.delete_subject(subject, permanent=True)
            else:
                # Soft delete
                self.client.delete_subject(subject)
            print(f"   ✅ Schema subject '{subject}' deleted")
            return True
        except Exception as e:
            error_str = str(e).lower()
            if "does not exist" in error_str or "not found" in error_str:
                print(f"   ⚠️  Schema subject '{subject}' does not exist")
                return True
            else:
                print(f"   ⚠️  Failed to delete schema subject '{subject}': {e}")
                return False


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    import yaml
    
    if config_path is None:
        backend_config = Path(__file__).parent.parent / "backend" / "config.yaml"
        if backend_config.exists():
            config_path = str(backend_config)
        else:
            return {}
    
    try:
        with open(config_path, 'r') as file:
            return yaml.safe_load(file) or {}
    except:
        return {}


def get_cloud_api_credentials() -> tuple:
    """Get Confluent Cloud Management API credentials from environment or config"""
    api_key = os.getenv('CONFLUENT_CLOUD_API_KEY')
    api_secret = os.getenv('CONFLUENT_CLOUD_API_SECRET')
    
    if not api_key or not api_secret:
        admin_config_path = Path(__file__).parent / "config.yaml"
        if admin_config_path.exists():
            import yaml
            with open(admin_config_path, 'r') as f:
                admin_config = yaml.safe_load(f)
                api_key = admin_config.get('cloud_api_key') or admin_config.get('CONFLUENT_CLOUD_API_KEY')
                api_secret = admin_config.get('cloud_api_secret') or admin_config.get('CONFLUENT_CLOUD_API_SECRET')
    
    if not api_key or not api_secret:
        raise ValueError(
            "Confluent Cloud Management API credentials not found.\n"
            "Please set CONFLUENT_CLOUD_API_KEY and CONFLUENT_CLOUD_API_SECRET environment variables,\n"
            "or create admin/config.yaml with cloud_api_key and cloud_api_secret."
        )
    
    return api_key, api_secret


def main():
    """Main function to delete all infrastructure"""
    print("=" * 60)
    print("Confluent Cloud Infrastructure Teardown")
    print("=" * 60)
    print()
    
    # Load admin config if it exists
    admin_config = {}
    admin_config_path = Path(__file__).parent / "config.yaml"
    if admin_config_path.exists():
        import yaml
        with open(admin_config_path, 'r') as f:
            admin_config = yaml.safe_load(f) or {}
    
    # Configuration - check env vars first, then admin config, then defaults
    environment_name = (
        os.getenv('CONFLUENT_ENVIRONMENT_NAME') or 
        admin_config.get('environment_name') or 
        'f1-leaderboard-env'
    )
    kafka_cluster_name = (
        os.getenv('CONFLUENT_KAFKA_CLUSTER_NAME') or 
        admin_config.get('kafka_cluster_name') or 
        'f1-leaderboard-cluster'
    )
    flink_pool_name = (
        os.getenv('CONFLUENT_FLINK_POOL_NAME') or 
        admin_config.get('flink_pool_name') or 
        'f1-leaderboard-flink-pool'
    )
    service_account_name = "f1-leaderboard-admin-sa"
    
    # Get Cloud Management API credentials
    try:
        cloud_api_key, cloud_api_secret = get_cloud_api_credentials()
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    # Initialize Cloud Manager
    cloud_manager = ConfluentCloudManager(cloud_api_key, cloud_api_secret)
    
    # Find resources and store in dictionary
    print("Step 1: Finding resources...")
    found_resources = {
        'environment': None,
        'cluster': None,
        'flink_pool': None,
        'service_account': None,
        'schema_subjects': [],
        'topics': []
    }
    
    env_id = cloud_manager._find_environment_by_name(environment_name)
    if not env_id:
        print(f"⚠️  Environment '{environment_name}' not found. Nothing to delete.")
        sys.exit(0)
    print(f"   ✅ Found environment: {environment_name} (ID: {env_id})")
    found_resources['environment'] = {'id': env_id, 'name': environment_name}
    
    cluster_id = cloud_manager._find_cluster_by_name(env_id, kafka_cluster_name)
    if cluster_id:
        print(f"   ✅ Found Kafka cluster: {kafka_cluster_name} (ID: {cluster_id})")
        found_resources['cluster'] = {'id': cluster_id, 'name': kafka_cluster_name}
    else:
        print(f"   ⚠️  Kafka cluster '{kafka_cluster_name}' not found")
    
    flink_pool_id = cloud_manager._find_flink_pool_by_name(env_id, flink_pool_name)
    if flink_pool_id:
        print(f"   ✅ Found Flink compute pool: {flink_pool_name} (ID: {flink_pool_id})")
        found_resources['flink_pool'] = {'id': flink_pool_id, 'name': flink_pool_name}
    else:
        print(f"   ⚠️  Flink compute pool '{flink_pool_name}' not found")
    
    service_account_id = cloud_manager._find_service_account_by_name(service_account_name)
    if service_account_id:
        print(f"   ✅ Found service account: {service_account_name} (ID: {service_account_id})")
        found_resources['service_account'] = {'id': service_account_id, 'name': service_account_name}
    else:
        print(f"   ⚠️  Service account '{service_account_name}' not found")
    
    print()
    
    # Step 2: Delete Schema Registry subjects (only if cluster was found and subjects exist)
    if found_resources['cluster']:
        try:
            config = load_config()
            if config.get('kafka', {}).get('schema_registry_url') and config.get('kafka', {}).get('schema_registry_api_key'):
                schema_registry_url = config['kafka']['schema_registry_url'].rstrip('/')
                if not schema_registry_url.startswith('http'):
                    schema_registry_url = f"https://{schema_registry_url}"
                
                schema_registry_config = {
                    'url': schema_registry_url,
                    'basic.auth.user.info': f"{config['kafka']['schema_registry_api_key']}:{config['kafka'].get('schema_registry_secret', '')}"
                }
                
                schema_admin = SchemaRegistryAdmin(schema_registry_config)
                
                # First check which schema subjects exist
                existing_subjects = []
                for subject_key, subject_name in SCHEMA_SUBJECTS.items():
                    if schema_admin.subject_exists(subject_name):
                        existing_subjects.append(subject_name)
                        found_resources['schema_subjects'].append(subject_name)
                
                # Only proceed with Step 2 if there are subjects to delete
                if existing_subjects:
                    print("Step 2: Deleting Schema Registry Subjects...")
                    for subject_name in existing_subjects:
                        schema_admin.delete_subject(subject_name, permanent=False)
                    print()
                # Skip Step 2 if no subjects exist
            else:
                # No credentials, skip silently
                pass
        except Exception as e:
            # If there's an error checking, skip silently
            pass
    
    # Step 2 is skipped if no subjects found or cluster not found
    
    # Step 3: Delete Kafka topics (only if cluster was found and topics exist)
    if found_resources['cluster']:
        try:
            config = load_config()
            if config.get('kafka', {}).get('bootstrap.servers') and config.get('kafka', {}).get('sasl.username'):
                kafka_config = config['kafka'].copy()
                # Remove non-Kafka config for AdminClient
                admin_kafka_config = kafka_config.copy()
                admin_kafka_config.pop('schema_registry_url', None)
                admin_kafka_config.pop('schema_registry_api_key', None)
                admin_kafka_config.pop('schema_registry_secret', None)
                admin_kafka_config.pop('topics', None)
                admin_kafka_config.pop('consumer_group', None)
                
                kafka_admin = KafkaAdmin(admin_kafka_config)
                
                # Get topic names from config and check if they exist
                topic_names = []
                if 'topics' in config.get('kafka', {}):
                    positions_topic = config['kafka']['topics'].get('positions')
                    if positions_topic:
                        topic_names.append(positions_topic)
                
                if topic_names:
                    # Check which topics exist before trying to delete
                    existing_topics = []
                    for topic in topic_names:
                        if kafka_admin.topic_exists(topic):
                            existing_topics.append(topic)
                            found_resources['topics'].append(topic)
                    
                    # Only proceed with Step 3 if there are topics to delete
                    if existing_topics:
                        print("Step 3: Deleting Kafka Topics...")
                        kafka_admin.delete_topics(existing_topics)
                        print()
                    # Skip Step 3 if no topics exist
        except Exception as e:
            # If there's an error checking, skip silently
            pass
    
    # Step 3 is skipped if no topics found or cluster not found
    
    # Step 4: Delete Flink compute pool
    print("Step 4: Deleting Flink Compute Pool...")
    if found_resources['flink_pool']:
        cloud_manager.delete_flink_compute_pool(env_id, found_resources['flink_pool']['id'])
    else:
        print("   ⚠️  Flink compute pool not found. Skipping.")
    print()
    
    # Step 5: Delete Kafka cluster
    print("Step 5: Deleting Kafka Cluster...")
    if found_resources['cluster']:
        cloud_manager.delete_kafka_cluster(env_id, found_resources['cluster']['id'])
    else:
        print("   ⚠️  Kafka cluster not found. Skipping.")
    print()
    
    # Step 6: Delete service account
    print("Step 6: Deleting Service Account...")
    if found_resources['service_account']:
        cloud_manager.delete_service_account(found_resources['service_account']['id'])
    else:
        print("   ⚠️  Service account not found. Skipping.")
    print()
    
    # Step 7: Delete environment
    print("Step 7: Deleting Environment...")
    if found_resources['environment']:
        cloud_manager.delete_environment(found_resources['environment']['id'])
    else:
        print("   ⚠️  Environment not found. Skipping.")
    print()
    
    print("=" * 60)
    print("✅ Infrastructure teardown complete!")
    print("=" * 60)
    print()
    print("Note: Some resources may take a few minutes to fully delete.")
    print("      Check the Confluent Cloud console to verify deletion.")


if __name__ == "__main__":
    main()

