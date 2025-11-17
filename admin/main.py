"""
Confluent Admin Infrastructure Setup Script

This script creates the complete Confluent Cloud infrastructure:
- Environment
- Kafka Cluster (Basic) in us-east-1
- Flink Compute Pool in us-east-1
- Kafka Topic: f1-driver-positions
"""

import os
import sys
import json
import time
import base64
import requests
from typing import Dict, Any, Optional
from pathlib import Path

# Add parent directory to path to import backend modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka import KafkaException

# Import schemas from backend
try:
    from backend.schemas import LEADERBOARD_UPDATE_SCHEMA, CAR_METRICS_SCHEMA
    from backend.config import config as backend_config
except ImportError:
    # Fallback if running from admin directory
    sys.path.insert(0, str(Path(__file__).parent.parent / "backend"))
    from schemas import LEADERBOARD_UPDATE_SCHEMA, CAR_METRICS_SCHEMA
    from config import config as backend_config


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
    
    def create_environment(self, name: str) -> Optional[str]:
        """Create a Confluent Cloud environment or return existing one"""
        # Check if environment already exists first
        env_id = self._find_environment_by_name(name)
        if env_id:
            print(f"✅ Environment '{name}' already exists with ID: {env_id}")
            return env_id
        
        # Create new environment
        url = f"{self.BASE_URL}/org/v2/environments"
        payload = {"display_name": name}
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code == 201:
                env_id = response.json()['id']
                print(f"✅ Environment '{name}' created with ID: {env_id}")
                return env_id
            elif response.status_code == 409:
                # Environment was created between check and create, find it
                print(f"⚠️  Environment '{name}' was created concurrently. Searching...")
                env_id = self._find_environment_by_name(name)
                if env_id:
                    print(f"✅ Found environment '{name}' with ID: {env_id}")
                    return env_id
                else:
                    print(f"❌ Failed to find environment '{name}': {response.text}")
                    return None
            else:
                print(f"❌ Failed to create environment '{name}': {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating environment '{name}': {e}")
            return None
    
    def _find_environment_by_name(self, name: str) -> Optional[str]:
        """Find environment by name with pagination support"""
        url = f"{self.BASE_URL}/org/v2/environments"
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
                    environments = data.get('data', [])
                    
                    for env in environments:
                        if env.get('display_name') == name:
                            return env['id']
                    
                    # Check if there are more pages
                    metadata = data.get('metadata', {})
                    next_page_token = metadata.get('next', {}).get('page_token')
                    if not next_page_token:
                        break
                    page_token = next_page_token
                else:
                    # If pagination fails, try without pagination as fallback
                    response = requests.get(url, headers=self.headers)
                    if response.status_code == 200:
                        environments = response.json().get('data', [])
                        for env in environments:
                            if env.get('display_name') == name:
                                return env['id']
                            break
        except Exception as e:
            print(f"⚠️  Error searching for environment: {e}")
        return None
    
    def create_kafka_cluster(self, env_id: str, name: str, cloud: str, region: str) -> Optional[str]:
        """Create a Basic Kafka cluster in Confluent Cloud or return existing one"""
        # Check if cluster already exists first
        cluster_id = self._find_cluster_by_name(env_id, name)
        if cluster_id:
            print(f"✅ Kafka cluster '{name}' already exists with ID: {cluster_id}")
            # Check if cluster is ready
            print(f"   ⏳ Checking cluster status...")
            self._wait_for_cluster_ready(env_id, cluster_id, max_wait=30)
            return cluster_id
        
        # Create new cluster
        url = f"{self.BASE_URL}/cmk/v2/clusters"
        # For Basic clusters, we use the standard cluster creation endpoint
        # Basic is the default cluster type
        payload = {
            "spec": {
                "display_name": name,
                "cloud": cloud.upper(),
                "region": region,
                "availability": "SINGLE_ZONE",
                "config": {
                    "kind": "Basic"
                },
                "environment": {
                    "id": env_id
                }
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            # 201 = Created, 202 = Accepted (async creation)
            if response.status_code in [201, 202]:
                cluster_data = response.json()
                cluster_id = cluster_data.get('id')
                if not cluster_id:
                    # Try to extract from metadata if id is not at root
                    cluster_id = cluster_data.get('metadata', {}).get('self', '').split('/')[-1]
                
                if cluster_id:
                    print(f"✅ Kafka cluster '{name}' created/accepted with ID: {cluster_id}")
                    print(f"   ⏳ Waiting for cluster to be ready...")
                    self._wait_for_cluster_ready(env_id, cluster_id)
                    return cluster_id
                else:
                    print(f"❌ Failed to extract cluster ID from response: {response.text}")
                    return None
            elif response.status_code == 409:
                # Cluster was created between check and create, find it
                print(f"⚠️  Kafka cluster '{name}' was created concurrently. Searching...")
                cluster_id = self._find_cluster_by_name(env_id, name)
                if cluster_id:
                    print(f"✅ Found cluster '{name}' with ID: {cluster_id}")
                    self._wait_for_cluster_ready(env_id, cluster_id, max_wait=30)
                    return cluster_id
                else:
                    print(f"❌ Failed to find cluster '{name}': {response.text}")
                    return None
            else:
                print(f"❌ Failed to create Kafka cluster '{name}': {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating Kafka cluster '{name}': {e}")
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
    
    def _wait_for_cluster_ready(self, env_id: str, cluster_id: str, max_wait: int = 600):
        """Wait for cluster to be ready (UP or PROVISIONED status)"""
        url = f"{self.BASE_URL}/cmk/v2/clusters/{cluster_id}"
        params = {"environment": env_id}
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    status = response.json().get('status', {}).get('phase', '')
                    if status in ['UP', 'PROVISIONED']:
                        print(f"   ✅ Cluster is ready! (status: {status})")
                        return True
                    elif status == 'PROVISIONING':
                        print(f"   ⏳ Cluster status: {status}...")
                        time.sleep(10)
                    else:
                        print(f"   ⚠️  Cluster status: {status}")
                        time.sleep(10)
                else:
                    time.sleep(5)
            except Exception as e:
                print(f"   ⚠️  Error checking cluster status: {e}")
                time.sleep(5)
        
        print(f"   ⚠️  Cluster did not become ready within {max_wait} seconds")
        return False
    
    def create_flink_compute_pool(self, env_id: str, name: str, cloud: str, region: str, max_cfu: int = 5) -> Optional[str]:
        """Create a Flink compute pool in Confluent Cloud or return existing one"""
        # Check if compute pool already exists first
        compute_pool_id = self._find_flink_pool_by_name(env_id, name)
        if compute_pool_id:
            print(f"✅ Flink compute pool '{name}' already exists with ID: {compute_pool_id}")
            return compute_pool_id
        
        # Create new compute pool
        url = f"{self.BASE_URL}/fcpm/v2/compute-pools"
        payload = {
            "spec": {
                "display_name": name,
                "cloud": cloud.upper(),
                "region": region,
                "max_cfu": max_cfu,
                "environment": {
                    "id": env_id
                }
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            # 201 = Created, 202 = Accepted (async creation)
            if response.status_code in [201, 202]:
                pool_data = response.json()
                compute_pool_id = pool_data.get('id')
                if not compute_pool_id:
                    # Try to extract from metadata if id is not at root
                    compute_pool_id = pool_data.get('metadata', {}).get('self', '').split('/')[-1]
                
                if compute_pool_id:
                    print(f"✅ Flink compute pool '{name}' created/accepted with ID: {compute_pool_id}")
                    print(f"   ⏳ Waiting for compute pool to be ready...")
                    self._wait_for_flink_pool_ready(env_id, compute_pool_id)
                    return compute_pool_id
                else:
                    print(f"❌ Failed to extract compute pool ID from response: {response.text}")
                    return None
            elif response.status_code == 409:
                # Compute pool was created between check and create, find it
                print(f"⚠️  Flink compute pool '{name}' was created concurrently. Searching...")
                compute_pool_id = self._find_flink_pool_by_name(env_id, name)
                if compute_pool_id:
                    print(f"✅ Found Flink compute pool '{name}' with ID: {compute_pool_id}")
                    self._wait_for_flink_pool_ready(env_id, compute_pool_id, max_wait=30)
                    return compute_pool_id
                else:
                    print(f"❌ Failed to find Flink compute pool '{name}': {response.text}")
                    return None
            else:
                print(f"❌ Failed to create Flink compute pool '{name}': {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating Flink compute pool '{name}': {e}")
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
    
    def _wait_for_flink_pool_ready(self, env_id: str, pool_id: str, max_wait: int = 600):
        """Wait for Flink compute pool to be ready (PROVISIONED status)"""
        url = f"{self.BASE_URL}/fcpm/v2/compute-pools/{pool_id}"
        params = {"environment": env_id}
        
        start_time = time.time()
        while time.time() - start_time < max_wait:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    status = response.json().get('status', {}).get('phase', '')
                    if status == 'PROVISIONED':
                        print(f"   ✅ Flink compute pool is ready! (status: {status})")
                        return True
                    elif status == 'PROVISIONING':
                        print(f"   ⏳ Flink compute pool status: {status}...")
                        time.sleep(10)
                    else:
                        print(f"   ⚠️  Flink compute pool status: {status}")
                        time.sleep(10)
                else:
                    time.sleep(5)
            except Exception as e:
                print(f"   ⚠️  Error checking Flink compute pool status: {e}")
                time.sleep(5)
        
        print(f"   ⚠️  Flink compute pool did not become ready within {max_wait} seconds")
        return False
    
    def _find_service_account_by_name(self, name: str) -> Optional[str]:
        """Helper method to find a service account by name"""
        url = f"{self.BASE_URL}/iam/v2/service-accounts"
        try:
            # Try with pagination to get all service accounts
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
                        # Check multiple possible locations for the name
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
                    
                    # Check if there are more pages
                    metadata = data.get('metadata', {})
                    next_page_token = metadata.get('next', {}).get('page_token')
                    if not next_page_token:
                        break
                    page_token = next_page_token
                else:
                    # If pagination fails, try without pagination
                    response = requests.get(url, headers=self.headers)
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
                    break
        except Exception as e:
            print(f"⚠️  Error searching for service account: {e}")
        return None
    
    def create_or_get_service_account(self, name: str = "f1-leaderboard-admin-sa", description: str = "Service account for F1 Leaderboard infrastructure management") -> Optional[str]:
        """Create or get a service account for managing infrastructure"""
        # First, check if service account already exists
        existing_sa_id = self._find_service_account_by_name(name)
        if existing_sa_id:
            print(f"✅ Service account '{name}' already exists with ID: {existing_sa_id}")
            return existing_sa_id
        
        # Create new service account
        url = f"{self.BASE_URL}/iam/v2/service-accounts"
        payload = {
            "display_name": name,
            "description": description
        }
        # Try with spec wrapper if needed
        payload_alt = {
            "spec": {
                "display_name": name,
                "description": description
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code in [200, 201]:
                data = response.json()
                sa_id = data.get('id')
                if sa_id:
                    print(f"✅ Service account '{name}' created with ID: {sa_id}")
                    return sa_id
            elif response.status_code == 409:
                # Service account already exists, find it
                print(f"⚠️  Service account '{name}' already exists. Searching...")
                existing_sa_id = self._find_service_account_by_name(name)
                if existing_sa_id:
                    print(f"✅ Found existing service account '{name}' with ID: {existing_sa_id}")
                    return existing_sa_id
                else:
                    print(f"⚠️  Service account exists but could not retrieve ID. Response: {response.text}")
                    return None
            # Try alternative format if first fails
            if response.status_code not in [200, 201, 409]:
                response_alt = requests.post(url, headers=self.headers, json=payload_alt)
                if response_alt.status_code in [200, 201]:
                    data = response_alt.json()
                    sa_id = data.get('id')
                    if sa_id:
                        print(f"✅ Service account '{name}' created with ID: {sa_id}")
                        return sa_id
                elif response_alt.status_code == 409:
                    # Try to find existing service account
                    existing_sa_id = self._find_service_account_by_name(name)
                    if existing_sa_id:
                        print(f"✅ Found existing service account '{name}' with ID: {existing_sa_id}")
                        return existing_sa_id
                print(f"❌ Failed to create service account: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating service account: {e}")
            return None
    
    def get_organization_id(self) -> Optional[str]:
        """Get the organization ID from the API"""
        url = f"{self.BASE_URL}/org/v2/organizations"
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                orgs = data.get('data', [])
                if orgs:
                    org_id = orgs[0].get('id')
                    return org_id
        except Exception as e:
            print(f"⚠️  Error getting organization ID: {e}")
        return None
    
    def check_role_binding_exists(self, service_account_id: str, role_name: str, crn_pattern: str) -> bool:
        """Check if a role binding already exists"""
        url = f"{self.BASE_URL}/iam/v2/role-bindings"
        params = {
            "principal": f"User:{service_account_id}",
            "role_name": role_name,
            "crn_pattern": crn_pattern
        }
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                bindings = data.get('data', [])
                return len(bindings) > 0
        except:
            pass
        return False
    
    def assign_environment_admin_role(self, service_account_id: str, env_id: str) -> bool:
        """Assign EnvironmentAdmin role to a service account for an environment"""
        url = f"{self.BASE_URL}/iam/v2/role-bindings"
        
        # Get organization ID
        org_id = self.get_organization_id()
        if not org_id:
            print("⚠️  Could not get organization ID, trying with wildcard...")
            org_id = "*"
        
        crn_pattern = f"crn://confluent.cloud/organization={org_id}/environment={env_id}"
        
        # Check if role binding already exists
        if self.check_role_binding_exists(service_account_id, "EnvironmentAdmin", crn_pattern):
            print(f"✅ EnvironmentAdmin role already assigned to service account")
            return True
        
        # Try with crn_pattern first
        payload = {
            "principal": f"User:{service_account_id}",
            "role_name": "EnvironmentAdmin",
            "crn_pattern": crn_pattern
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code in [200, 201]:
                print(f"✅ Assigned EnvironmentAdmin role to service account")
                return True
            elif response.status_code == 409:
                print(f"✅ EnvironmentAdmin role already assigned to service account")
                return True
            else:
                # Try alternative format with resource_name
                payload_alt = {
                    "principal": f"User:{service_account_id}",
                    "role_name": "EnvironmentAdmin",
                    "resource_name": crn_pattern
                }
                response_alt = requests.post(url, headers=self.headers, json=payload_alt)
                if response_alt.status_code in [200, 201, 409]:
                    print(f"✅ Assigned EnvironmentAdmin role to service account (alternative format)")
                    return True
                print(f"⚠️  Failed to assign EnvironmentAdmin role: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"⚠️  Error assigning EnvironmentAdmin role: {e}")
            return False
    
    def assign_cloud_cluster_admin_role(self, service_account_id: str, env_id: str, cluster_id: str) -> bool:
        """Assign CloudClusterAdmin role to a service account for a cluster"""
        url = f"{self.BASE_URL}/iam/v2/role-bindings"
        
        # Get organization ID
        org_id = self.get_organization_id()
        if not org_id:
            print("⚠️  Could not get organization ID, trying with wildcard...")
            org_id = "*"
        
        crn_pattern = f"crn://confluent.cloud/organization={org_id}/environment={env_id}/cloud-cluster={cluster_id}"
        
        # Check if role binding already exists
        if self.check_role_binding_exists(service_account_id, "CloudClusterAdmin", crn_pattern):
            print(f"✅ CloudClusterAdmin role already assigned to service account")
            return True
        
        # Try with crn_pattern first
        payload = {
            "principal": f"User:{service_account_id}",
            "role_name": "CloudClusterAdmin",
            "crn_pattern": crn_pattern
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            if response.status_code in [200, 201]:
                print(f"✅ Assigned CloudClusterAdmin role to service account")
                return True
            elif response.status_code == 409:
                print(f"✅ CloudClusterAdmin role already assigned to service account")
                return True
            else:
                # Try alternative format with resource_name
                payload_alt = {
                    "principal": f"User:{service_account_id}",
                    "role_name": "CloudClusterAdmin",
                    "resource_name": crn_pattern
                }
                response_alt = requests.post(url, headers=self.headers, json=payload_alt)
                if response_alt.status_code in [200, 201, 409]:
                    print(f"✅ Assigned CloudClusterAdmin role to service account (alternative format)")
                    return True
                print(f"⚠️  Failed to assign CloudClusterAdmin role: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"⚠️  Error assigning CloudClusterAdmin role: {e}")
            return False
    
    def list_api_keys_for_service_account(self, service_account_id: str) -> list:
        """List all API keys owned by a service account with pagination support"""
        url = f"{self.BASE_URL}/iam/v2/api-keys"
        api_keys = []
        
        try:
            # Try different owner formats
            owner_formats = [
                f"User:{service_account_id}",
                f"sa-{service_account_id}",
                service_account_id
            ]
            
            for owner_format in owner_formats:
                page_size = 100
                page_token = None
                found_keys = False
                
                while True:
                    params = {"owner": owner_format, "page_size": page_size}
                    if page_token:
                        params["page_token"] = page_token
                    
                    response = requests.get(url, headers=self.headers, params=params)
                    if response.status_code == 200:
                        data = response.json()
                        keys = data.get('data', [])
                        if keys:
                            api_keys.extend(keys)
                            found_keys = True
                        
                            # Check if there are more pages
                            metadata = data.get('metadata', {})
                            next_page_token = metadata.get('next', {}).get('page_token')
                            if not next_page_token:
                                break
                            page_token = next_page_token
                        else:
                            # If pagination fails, try without pagination as fallback
                            params_fallback = {"owner": owner_format}
                            response_fallback = requests.get(url, headers=self.headers, params=params_fallback)
                            if response_fallback.status_code == 200:
                                keys = response_fallback.json().get('data', [])
                                if keys:
                                    api_keys.extend(keys)
                                    found_keys = True
                            break
                    
                    if found_keys:
                            break
        except Exception as e:
            print(f"⚠️  Error listing API keys: {e}")
        
        return api_keys
    
    def delete_api_key(self, api_key_id: str) -> bool:
        """Delete an API key"""
        url = f"{self.BASE_URL}/iam/v2/api-keys/{api_key_id}"
        try:
            response = requests.delete(url, headers=self.headers)
            if response.status_code in [200, 204]:
                return True
            else:
                print(f"⚠️  Failed to delete API key {api_key_id}: {response.status_code} - {response.text}")
                return False
        except Exception as e:
            print(f"⚠️  Error deleting API key: {e}")
            return False
    
    def get_or_create_kafka_api_key(self, cluster_id: str, env_id: str, service_account_id: str, expected_api_key: str = None, description: str = "Kafka API key for admin script") -> Optional[tuple]:
        """Get existing Kafka API key or create a new one, verifying against expected key"""
        # List all API keys for the service account
        api_keys = self.list_api_keys_for_service_account(service_account_id)
        
        # Find Kafka API keys for this cluster
        kafka_api_keys = []
        for key in api_keys:
            resource = key.get('spec', {}).get('resource', {})
            if resource.get('kind') == 'Cluster' and resource.get('id') == cluster_id:
                kafka_api_keys.append(key)
        
        # If we have an expected API key, check if it exists
        if expected_api_key:
            for key in kafka_api_keys:
                if key.get('id') == expected_api_key:
                    print(f"✅ Found existing Kafka API key matching config: {expected_api_key}")
                    # Secret not available from API, will use from config
                    return (expected_api_key, None)
        
        # If expected key doesn't match or doesn't exist, delete old keys and create new one
        if kafka_api_keys:
            print(f"   Found {len(kafka_api_keys)} existing Kafka API key(s) that don't match config, deleting...")
            for key in kafka_api_keys:
                key_id = key.get('id')
                self.delete_api_key(key_id)
        
        # Create new API key
        print(f"   Creating new Kafka API key...")
        return self.create_kafka_api_key(cluster_id, env_id, service_account_id, description)
    
    def create_kafka_api_key(self, cluster_id: str, env_id: str, service_account_id: str, description: str = "Kafka API key for admin script") -> Optional[tuple]:
        """Create a Kafka API key for the cluster using a service account"""
        url = f"{self.BASE_URL}/iam/v2/api-keys"
        
        # According to Confluent Cloud API docs, both owner and resource are required
        payload = {
            "spec": {
                "display_name": description,
                "description": description,
                "owner": {
                    "id": service_account_id,
                    "kind": "ServiceAccount"
                },
                "resource": {
                    "id": cluster_id,
                    "api_version": "cmk/v2",
                    "kind": "Cluster",
                    "environment": env_id
                }
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            # 201 = Created, 202 = Accepted (async creation)
            if response.status_code in [200, 201, 202]:
                data = response.json()
                api_key = data.get('id')
                api_secret = data.get('spec', {}).get('secret')
                if api_key and api_secret:
                    print(f"✅ Kafka API key created: {api_key}")
                    time.sleep(20)
                    return (api_key, api_secret)
                else:
                    print(f"⚠️  API key created but secret not in response. Key ID: {api_key}")
                    print(f"   You may need to retrieve the secret separately or it was only shown once.")
                    return (api_key, None)
            else:
                print(f"❌ Failed to create Kafka API key: {response.status_code} - {response.text}")
                return None
        except Exception as e:
            print(f"❌ Error creating Kafka API key: {e}")
            return None
    
    def get_schema_registry_info(self, env_id: str, cluster_id: str = None) -> tuple:
        """Get the Schema Registry cluster ID and endpoint URL from the environment"""
        # Use the srcm/v3 API to get Schema Registry information for the environment
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
                # Get the first Schema Registry cluster
                item = items[0]
                sr_cluster_id = item.get('id')
                
                # Try multiple possible locations for the endpoint
                spec = item.get('spec', {})
                status = item.get('status', {})
                
                # Try spec.http.endpoint (preferred location)
                sr_endpoint = spec.get('http', {}).get('endpoint')
                
                # If not found, try alternative locations
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
                
                if sr_cluster_id:
                    print(f"   Found Schema Registry cluster: {sr_cluster_id}")
                if sr_endpoint:
                    print(f"   Schema Registry endpoint: {sr_endpoint}")
                else:
                    # Debug: print the structure to help diagnose
                    print(f"   ⚠️  Schema Registry endpoint not found in API response")
                    print(f"   Debug - spec keys: {list(spec.keys()) if spec else 'None'}")
                    print(f"   Debug - status keys: {list(status.keys()) if status else 'None'}")
            else:
                print(f"   ⚠️  No Schema Registry clusters found for environment {env_id}")
        except requests.exceptions.HTTPError as e:
            print(f"   ⚠️  Failed to get Schema Registry info: {e.response.status_code} - {e.response.text}")
        except Exception as e:
            print(f"   ⚠️  Error getting Schema Registry info: {e}")
        
        if not sr_cluster_id:
            print("⚠️  No Schema Registry found for this environment.")
            print("   Schema Registry may need to be enabled for the environment.")
        
        return sr_cluster_id, sr_endpoint
    
    def get_schema_registry_cluster_id(self, env_id: str, cluster_id: str = None) -> Optional[str]:
        """Get the Schema Registry cluster ID"""
        sr_cluster_id, _ = self.get_schema_registry_info(env_id, cluster_id)
        return sr_cluster_id
    
    def get_or_create_schema_registry_api_key(self, env_id: str, service_account_id: str, expected_api_key: str = None, cluster_id: str = None, description: str = "Schema Registry API key for admin script") -> Optional[tuple]:
        """Get existing Schema Registry API key or create a new one, verifying against expected key"""
        # Schema Registry API keys are scoped to the environment, not a specific cluster
        # Get Schema Registry cluster ID for reference, but API key is environment-scoped
        sr_cluster_id, _ = self.get_schema_registry_info(env_id, cluster_id)
        if not sr_cluster_id:
            print("⚠️  Could not find Schema Registry cluster. Schema Registry may not be enabled.")
            return None
        
        # List all API keys for the service account
        api_keys = self.list_api_keys_for_service_account(service_account_id)
        
        # Find Schema Registry API keys for this environment
        # Schema Registry API keys can be identified by:
        # 1. kind: "SchemaRegistry" with environment matching
        # 2. kind: "SchemaRegistry" with cluster ID matching (legacy)
        sr_api_keys = []
        for key in api_keys:
            resource = key.get('spec', {}).get('resource', {})
            resource_kind = resource.get('kind')
            resource_env = resource.get('environment')
            
            # Check if it's a Schema Registry API key for this environment
            if resource_kind == 'SchemaRegistry':
                # Check if it's for this environment (environment-scoped)
                if resource_env == env_id:
                    sr_api_keys.append(key)
                # Also check if it matches the cluster ID (cluster-scoped, legacy)
                elif resource.get('id') == sr_cluster_id:
                    sr_api_keys.append(key)
        
        # If we have an expected API key, check if it exists
        if expected_api_key:
            for key in sr_api_keys:
                if key.get('id') == expected_api_key:
                    print(f"✅ Found existing Schema Registry API key matching config: {expected_api_key}")
                    # Secret not available from API, will use from config
                    return (expected_api_key, None)
        
        # If expected key doesn't match or doesn't exist, delete old keys and create new one
        if sr_api_keys:
            print(f"   Found {len(sr_api_keys)} existing Schema Registry API key(s) that don't match config, deleting...")
            for key in sr_api_keys:
                key_id = key.get('id')
                self.delete_api_key(key_id)
        
        # Create new API key scoped to environment
        print(f"   Creating new Schema Registry API key (environment-scoped)...")
        return self.create_schema_registry_api_key(env_id, service_account_id, sr_cluster_id, description)
    
    def create_schema_registry_api_key(self, env_id: str, service_account_id: str, sr_cluster_id: str, description: str = "Schema Registry API key for admin script") -> Optional[tuple]:
        """Create a Schema Registry API key scoped to the environment"""
        # Schema Registry API keys should be scoped to the environment, not the cluster
        # However, we still need to reference the Schema Registry cluster ID
        url = f"{self.BASE_URL}/iam/v2/api-keys"
        
        # Schema Registry API keys are environment-scoped
        # The resource should reference the environment, not the cluster
        # Try format: SchemaRegistry kind with environment ID as the resource
        payload = {
            "spec": {
                "display_name": description,
                "description": description,
                "owner": {
                    "id": service_account_id,
                    "kind": "ServiceAccount"
                },
                "resource": {
                    "id": env_id,
                    "api_version": "srcm/v2",
                    "kind": "SchemaRegistry",
                    "environment": env_id
                }
            }
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            # 201 = Created, 202 = Accepted (async creation)
            if response.status_code in [200, 201, 202]:
                data = response.json()
                api_key = data.get('id')
                api_secret = data.get('spec', {}).get('secret')
                if api_key and api_secret:
                    print(f"✅ Schema Registry API key created (environment-scoped): {api_key}")
                    return (api_key, api_secret)
                else:
                    print(f"⚠️  API key created but secret not in response. Key ID: {api_key}")
                    print(f"   You may need to retrieve the secret separately or it was only shown once.")
                    return (api_key, None)
            else:
                # Try alternative: use Schema Registry cluster ID but with environment scope
                print(f"   First attempt failed ({response.status_code}), trying alternative format...")
                payload_alt = {
                    "spec": {
                        "display_name": description,
                        "description": description,
                        "owner": {
                            "id": service_account_id,
                            "kind": "ServiceAccount"
                        },
                        "resource": {
                            "id": sr_cluster_id,
                            "api_version": "srcm/v2",
                            "kind": "SchemaRegistry",
                            "environment": env_id
                        }
                    }
                }
                response_alt = requests.post(url, headers=self.headers, json=payload_alt)
                if response_alt.status_code in [200, 201, 202]:
                    data = response_alt.json()
                    api_key = data.get('id')
                    api_secret = data.get('spec', {}).get('secret')
                    if api_key and api_secret:
                        print(f"✅ Schema Registry API key created (alternative format): {api_key}")
                        return (api_key, api_secret)
                    else:
                        print(f"⚠️  API key created but secret not in response. Key ID: {api_key}")
                        return (api_key, None)
                else:
                    print(f"❌ Failed to create Schema Registry API key: {response.status_code} - {response.text}")
                    if response_alt.status_code not in [200, 201, 202]:
                        print(f"   Alternative format also failed: {response_alt.status_code} - {response_alt.text}")
                    return None
        except Exception as e:
            print(f"❌ Error creating Schema Registry API key: {e}")
            return None


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
    
    def create_topics(self, topic_names: list, num_partitions: int = 1) -> bool:
        """Create Kafka topics"""
        topics = [NewTopic(topic, num_partitions=num_partitions) for topic in topic_names]
        futures = self.admin_client.create_topics(topics)
        
        success = True
        for topic, future in futures.items():
            try:
                future.result(timeout=30)
                print(f"✅ Topic '{topic}' created successfully")
            except KafkaException as e:
                if "already exists" in str(e).lower() or "TopicExistsException" in str(e):
                    print(f"⚠️  Topic '{topic}' already exists")
                else:
                    print(f"❌ Failed to create topic '{topic}': {e}")
                    success = False
            except Exception as e:
                print(f"❌ Error creating topic '{topic}': {e}")
                success = False
        
        return success


class SchemaRegistryAdmin:
    """Manages Schema Registry subjects"""
    
    def __init__(self, schema_registry_config: Dict[str, str]):
        self.client = SchemaRegistryClient(schema_registry_config)
        self.config = schema_registry_config
    
    def register_schema(self, subject: str, schema_dict: dict, schema_type: str = "AVRO") -> Optional[int]:
        """Register a schema with Schema Registry and return the schema ID"""
        try:
            schema_str = json.dumps(schema_dict)
            schema = Schema(schema_str, schema_type=schema_type)
            schema_id = self.client.register_schema(subject_name=subject, schema=schema)
            print(f"✅ Schema registered: {subject} (ID: {schema_id})")
            return schema_id
        except Exception as e:
            error_str = str(e).lower()
            if "already exists" in error_str or "subject already exists" in error_str:
                try:
                    registered_schema = self.client.get_latest_version(subject)
                    print(f"✅ Schema already exists: {subject} (ID: {registered_schema.schema_id})")
                    return registered_schema.schema_id
                except:
                    return None
            else:
                print(f"❌ Failed to register schema for {subject}: {e}")
                return None


def load_config(config_path: str = None) -> Dict[str, Any]:
    """Load configuration from YAML file"""
    import yaml
    
    if config_path is None:
        # Try to find config in backend directory
        backend_config = Path(__file__).parent.parent / "backend" / "config.yaml"
        if backend_config.exists():
            config_path = str(backend_config)
        else:
            raise FileNotFoundError("Could not find config.yaml file")
    
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def update_backend_config(
    bootstrap_servers: str = None,
    kafka_api_key: str = None,
    kafka_api_secret: str = None,
    schema_registry_url: str = None,
    schema_registry_api_key: str = None,
    schema_registry_secret: str = None
) -> bool:
    """Update backend/config.yaml with new API keys and credentials"""
    import yaml
    
    backend_config_path = Path(__file__).parent.parent / "backend" / "config.yaml"
    
    try:
        # Load existing config
        if backend_config_path.exists():
            with open(backend_config_path, 'r') as file:
                config = yaml.safe_load(file) or {}
        else:
            config = {}
        
        # Ensure kafka section exists
        if 'kafka' not in config:
            config['kafka'] = {}
        
        # Update values if provided
        if bootstrap_servers:
            config['kafka']['bootstrap.servers'] = bootstrap_servers
        if kafka_api_key:
            config['kafka']['sasl.username'] = kafka_api_key
        if kafka_api_secret:
            config['kafka']['sasl.password'] = kafka_api_secret
        if schema_registry_url:
            config['kafka']['schema_registry_url'] = schema_registry_url
        if schema_registry_api_key:
            config['kafka']['schema_registry_api_key'] = schema_registry_api_key
        if schema_registry_secret:
            config['kafka']['schema_registry_secret'] = schema_registry_secret
        
        # Ensure required fields exist
        if 'security.protocol' not in config['kafka']:
            config['kafka']['security.protocol'] = 'SASL_SSL'
        if 'sasl.mechanism' not in config['kafka']:
            config['kafka']['sasl.mechanism'] = 'PLAIN'
        if 'topics' not in config['kafka']:
            config['kafka']['topics'] = {}
        if 'positions' not in config['kafka']['topics']:
            config['kafka']['topics']['positions'] = 'f1-driver-positions'
        if 'consumer_group' not in config['kafka']:
            config['kafka']['consumer_group'] = 'f1-leaderboard-consumer'
        
        # Write updated config
        with open(backend_config_path, 'w') as file:
            yaml.dump(config, file, default_flow_style=False, sort_keys=False)
        
        print(f"✅ Updated backend/config.yaml with new credentials")
        return True
    except Exception as e:
        print(f"⚠️  Failed to update backend/config.yaml: {e}")
        return False


def get_cloud_api_credentials() -> tuple:
    """Get Confluent Cloud Management API credentials from environment or config"""
    api_key = os.getenv('CONFLUENT_CLOUD_API_KEY')
    api_secret = os.getenv('CONFLUENT_CLOUD_API_SECRET')
    
    if not api_key or not api_secret:
        # Try to load from admin config if it exists
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
            "or create admin/config.yaml with cloud_api_key and cloud_api_secret.\n"
            "See admin/config.yaml.example for reference."
        )
    
    return api_key, api_secret


def main():
    """Main function to create all infrastructure"""
    print("=" * 60)
    print("Confluent Cloud Infrastructure Setup")
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
    cloud_provider = (
        os.getenv('CONFLUENT_CLOUD_PROVIDER') or 
        admin_config.get('cloud_provider') or 
        'aws'
    )
    region = (
        os.getenv('CONFLUENT_REGION') or 
        admin_config.get('region') or 
        'us-east-1'
    )
    max_cfu = int(
        os.getenv('CONFLUENT_FLINK_MAX_CFU') or 
        admin_config.get('flink_max_cfu') or 
        '5'
    )
    
    # Get Cloud Management API credentials
    try:
        cloud_api_key, cloud_api_secret = get_cloud_api_credentials()
    except ValueError as e:
        print(f"❌ {e}")
        sys.exit(1)
    
    # Initialize Cloud Manager
    cloud_manager = ConfluentCloudManager(cloud_api_key, cloud_api_secret)
    
    # Step 1: Create or get Environment
    print("Step 1: Checking/Creating Confluent Cloud Environment...")
    env_id = cloud_manager.create_environment(environment_name)
    if not env_id:
        print("❌ Failed to get or create environment. Exiting.")
        sys.exit(1)
    print()
    
    # Step 2: Create or get Kafka Cluster
    print("Step 2: Checking/Creating Kafka Cluster...")
    cluster_id = cloud_manager.create_kafka_cluster(env_id, kafka_cluster_name, cloud_provider, region)
    if not cluster_id:
        print("❌ Failed to get or create Kafka cluster. Exiting.")
        sys.exit(1)
    print()
    
    # Step 3: Create or get Flink Compute Pool
    print("Step 3: Checking/Creating Flink Compute Pool...")
    flink_pool_id = cloud_manager.create_flink_compute_pool(env_id, flink_pool_name, cloud_provider, region, max_cfu)
    if not flink_pool_id:
        print("❌ Failed to get or create Flink compute pool. Exiting.")
        sys.exit(1)
    print()
    
    # Step 4: Create service account and assign roles
    print("Step 4: Creating Service Account and Assigning Roles...")
    service_account_id = cloud_manager.create_or_get_service_account(
        "f1-leaderboard-admin-sa",
        "Service account for F1 Leaderboard infrastructure management"
    )
    if not service_account_id:
        print("❌ Failed to create or get service account. Exiting.")
        sys.exit(1)
    
    # Assign EnvironmentAdmin role
    print("   Assigning EnvironmentAdmin role...")
    cloud_manager.assign_environment_admin_role(service_account_id, env_id)
    
    # Assign CloudClusterAdmin role for the cluster
    print("   Assigning CloudClusterAdmin role...")
    cloud_manager.assign_cloud_cluster_admin_role(service_account_id, env_id, cluster_id)
    print()
    
    # Step 5: Get cluster details and create/verify API keys
    print("Step 5: Checking/Creating API Keys...")
    
    # Load existing config to check for expected API keys
    expected_kafka_api_key = None
    expected_sr_api_key = None
    bootstrap_servers = None
    schema_registry_url = None
    original_schema_registry_url = None  # Store original from config to preserve it
    
    try:
        config = load_config()
        expected_kafka_api_key = config['kafka'].get('sasl.username')
        expected_sr_api_key = config['kafka'].get('schema_registry_api_key')
        bootstrap_servers = config['kafka'].get('bootstrap.servers')
        schema_registry_url = config['kafka'].get('schema_registry_url')
        original_schema_registry_url = schema_registry_url  # Preserve original
    except:
        pass
    
    # Check if bootstrap_servers is a placeholder and treat it as None
    PLACEHOLDER_BOOTSTRAP = "BOOTSTRAP_SERVER_URL_FROM_API_KEY_FILE"
    if bootstrap_servers == PLACEHOLDER_BOOTSTRAP:
        print(f"   ⚠️  Found placeholder '{PLACEHOLDER_BOOTSTRAP}' in config, will fetch actual bootstrap servers from cluster API")
        bootstrap_servers = None
    
    # Get cluster details to extract bootstrap servers if not in config or is placeholder
    if not bootstrap_servers:
        print("   Getting bootstrap servers from cluster API...")
        cluster_details_url = f"{cloud_manager.BASE_URL}/cmk/v2/clusters/{cluster_id}"
        cluster_params = {"environment": env_id}
        try:
            cluster_response = requests.get(cluster_details_url, headers=cloud_manager.headers, params=cluster_params)
            if cluster_response.status_code == 200:
                cluster_data = cluster_response.json()
                bootstrap_servers = cluster_data.get('spec', {}).get('kafka_bootstrap_endpoint', '').replace('SASL_SSL://', '')
                if bootstrap_servers:
                    print(f"   ✅ Retrieved bootstrap servers from cluster API: {bootstrap_servers}")
                else:
                    print(f"   ⚠️  Bootstrap endpoint not found in cluster response")
            else:
                print(f"   ⚠️  Failed to get cluster details: {cluster_response.status_code} - {cluster_response.text}")
        except Exception as e:
            print(f"⚠️  Error getting cluster details: {e}")
    
    # Always get Schema Registry URL from API to compare with config
    # Then update config if they don't match
    discovered_sr_url = None
    print("   Getting Schema Registry endpoint from API...")
    sr_cluster_id, sr_endpoint = cloud_manager.get_schema_registry_info(env_id, cluster_id)
    if sr_endpoint:
        discovered_sr_url = sr_endpoint
        print(f"   ✅ Discovered Schema Registry endpoint from API: {discovered_sr_url}")
        
        # Compare with config value
        if original_schema_registry_url:
            # Normalize URLs for comparison (remove trailing slashes, ensure https)
            config_url_normalized = original_schema_registry_url.rstrip('/')
            if not config_url_normalized.startswith('http'):
                config_url_normalized = f"https://{config_url_normalized}"
            
            discovered_url_normalized = discovered_sr_url.rstrip('/')
            if not discovered_url_normalized.startswith('http'):
                discovered_url_normalized = f"https://{discovered_url_normalized}"
            
            if config_url_normalized == discovered_url_normalized:
                print(f"   ✅ Schema Registry URL in config matches discovered endpoint: {discovered_sr_url}")
                schema_registry_url = discovered_sr_url  # Use discovered URL (they match)
            else:
                print(f"   ⚠️  Schema Registry URL mismatch detected:")
                print(f"      Config: {original_schema_registry_url}")
                print(f"      API:    {discovered_sr_url}")
                print(f"   🔄 Updating config with discovered endpoint...")
                schema_registry_url = discovered_sr_url  # Use discovered URL (they don't match)
        else:
            # No URL in config, use discovered one
            print(f"   ℹ️  No Schema Registry URL in config, using discovered endpoint: {discovered_sr_url}")
            schema_registry_url = discovered_sr_url
    else:
        print("   ⚠️  Could not get Schema Registry endpoint from API.")
        # If we have a valid URL in config, use it
        if original_schema_registry_url and 'psrc-' in original_schema_registry_url and 'pkc-' not in original_schema_registry_url:
            print(f"   ℹ️  Using existing Schema Registry URL from config: {original_schema_registry_url}")
            schema_registry_url = original_schema_registry_url
        elif schema_registry_url and 'pkc-' in schema_registry_url:
            # Invalid Kafka URL, clear it
            schema_registry_url = None
            print("   ⚠️  Cleared incorrect Kafka URL from schema_registry_url")
    
    # Get or create Kafka API key using service account
    print("   Checking/Creating Kafka API key...")
    kafka_api_result = cloud_manager.get_or_create_kafka_api_key(
        cluster_id, 
        env_id, 
        service_account_id, 
        expected_api_key=expected_kafka_api_key,
        description="Kafka API key for F1 Leaderboard"
    )
    if kafka_api_result:
        kafka_api_key, kafka_api_secret = kafka_api_result
        if kafka_api_key:
            if kafka_api_secret:
                print(f"   ✅ Kafka API key ready: {kafka_api_key}")
            else:
                print(f"   ⚠️  Kafka API key found but secret not available. Using from config if available.")
                # If we found the key but no secret, try to use secret from config
                if expected_kafka_api_key == kafka_api_key:
                    try:
                        config = load_config()
                        kafka_api_secret = config['kafka'].get('sasl.password')
                    except:
                        pass
        else:
            kafka_api_key = None
            kafka_api_secret = None
    else:
        kafka_api_key = None
        kafka_api_secret = None
        print("   ⚠️  Failed to create Kafka API key. Will try to use existing config.")
    
    # Get or create Schema Registry API key using service account
    print("   Checking/Creating Schema Registry API key...")
    sr_api_result = cloud_manager.get_or_create_schema_registry_api_key(
        env_id, 
        service_account_id, 
        expected_api_key=expected_sr_api_key,
        cluster_id=cluster_id,
        description="Schema Registry API key for F1 Leaderboard"
    )
    if sr_api_result:
        sr_api_key, sr_api_secret = sr_api_result
        if sr_api_key:
            if sr_api_secret:
                print(f"   ✅ Schema Registry API key ready: {sr_api_key}")
            else:
                print(f"   ⚠️  Schema Registry API key found but secret not available. Using from config if available.")
                # If we found the key but no secret, try to use secret from config
                if expected_sr_api_key == sr_api_key:
                    try:
                        config = load_config()
                        sr_api_secret = config['kafka'].get('schema_registry_secret')
                    except:
                        pass
        else:
            sr_api_key = None
            sr_api_secret = None
    else:
        sr_api_key = None
        sr_api_secret = None
        print("   ⚠️  Failed to create Schema Registry API key. Will try to use existing config.")
    
    # Verify we have a valid schema registry URL at this point
    if not schema_registry_url or schema_registry_url == '' or 'pkc-' in schema_registry_url:
        # Fallback: try to use original from config if it's valid
        if original_schema_registry_url and 'psrc-' in original_schema_registry_url and 'pkc-' not in original_schema_registry_url:
            schema_registry_url = original_schema_registry_url
            print(f"   ℹ️  Using Schema Registry URL from config (API discovery failed): {schema_registry_url}")
        elif discovered_sr_url:
            schema_registry_url = discovered_sr_url
            print(f"   ℹ️  Using previously discovered Schema Registry endpoint: {schema_registry_url}")
        else:
            print("   ⚠️  No valid Schema Registry URL available")
    
    # Update backend/config.yaml with the newly created credentials
    # Update schema_registry_url if:
    # 1. We discovered a URL from API and it's different from config, OR
    # 2. Config doesn't have a valid URL, OR
    # 3. Config has an invalid URL
    should_update_sr_url = False
    if discovered_sr_url:
        # We have a discovered URL from API
        if original_schema_registry_url:
            # Compare normalized URLs
            config_url_normalized = original_schema_registry_url.rstrip('/')
            if not config_url_normalized.startswith('http'):
                config_url_normalized = f"https://{config_url_normalized}"
            
            discovered_url_normalized = discovered_sr_url.rstrip('/')
            if not discovered_url_normalized.startswith('http'):
                discovered_url_normalized = f"https://{discovered_url_normalized}"
            
            if config_url_normalized != discovered_url_normalized:
                # URLs don't match - update config
                should_update_sr_url = True
                print(f"   🔄 Will update config with discovered Schema Registry URL: {discovered_sr_url}")
            else:
                # URLs match - no update needed
                should_update_sr_url = False
                print(f"   ✅ Schema Registry URL in config is correct, no update needed")
        else:
            # No URL in config - update it
            should_update_sr_url = True
            print(f"   🔄 Will add discovered Schema Registry URL to config: {discovered_sr_url}")
    elif schema_registry_url and schema_registry_url != '' and 'pkc-' not in schema_registry_url:
        # We have a valid URL (from config fallback) but didn't discover from API
        # Only update if config doesn't have it or it's invalid
        if not original_schema_registry_url or 'pkc-' in original_schema_registry_url:
            should_update_sr_url = True
        else:
            should_update_sr_url = False
    
    if (kafka_api_key and kafka_api_secret and bootstrap_servers) or (sr_api_key and sr_api_secret):
        print()
        print("   Updating backend/config.yaml with new credentials...")
        update_backend_config(
            bootstrap_servers=bootstrap_servers,
            kafka_api_key=kafka_api_key,
            kafka_api_secret=kafka_api_secret,
            schema_registry_url=schema_registry_url if should_update_sr_url else None,
            schema_registry_api_key=sr_api_key,
            schema_registry_secret=sr_api_secret
        )
    
    print()
    
    # Step 6: Create topics
    print("Step 6: Setting up Kafka Topics...")
    
    # Use the cluster API keys created in Step 5 for topic creation
    # These keys have the proper permissions to create topics
    try:
        config = load_config()
        
        # Build Kafka config for AdminClient
        # Prefer newly created API keys, fall back to config.yaml if not available
        admin_kafka_config = {}
        
        # Get bootstrap servers (from config or cluster details)
        # Check for placeholder in config as well
        config_bootstrap = config.get('kafka', {}).get('bootstrap.servers')
        if config_bootstrap == PLACEHOLDER_BOOTSTRAP:
            config_bootstrap = None
        
        if bootstrap_servers:
            admin_kafka_config['bootstrap.servers'] = bootstrap_servers
        elif config_bootstrap:
            admin_kafka_config['bootstrap.servers'] = config_bootstrap
        else:
            print("   ⚠️  bootstrap.servers not found")
        
        # Use newly created Kafka API keys if available, otherwise fall back to config
        if kafka_api_key and kafka_api_secret:
            admin_kafka_config['sasl.username'] = kafka_api_key
            admin_kafka_config['sasl.password'] = kafka_api_secret
            print("   Using newly created Kafka API keys for topic creation...")
        elif config.get('kafka', {}).get('sasl.username') and config.get('kafka', {}).get('sasl.password'):
            admin_kafka_config['sasl.username'] = config['kafka']['sasl.username']
            admin_kafka_config['sasl.password'] = config['kafka']['sasl.password']
            print("   Using Kafka credentials from backend/config.yaml for topic creation...")
        else:
            print("   ⚠️  Kafka API credentials not available")
        
        # Add required security settings
        admin_kafka_config['security.protocol'] = config.get('kafka', {}).get('security.protocol', 'SASL_SSL')
        admin_kafka_config['sasl.mechanism'] = config.get('kafka', {}).get('sasl.mechanism', 'PLAIN')
        
        # Suppress librdkafka logs (connection disconnection messages)
        admin_kafka_config['log_level'] = 0  # 0 = emerg (suppress all logs)
        
        # Get topic names (positions topic, and optionally car metrics topics)
        topic_names = [config['kafka']['topics']['positions']]
        
        # Check if anomaly detection feature is enabled
        anomaly_detection_enabled = False
        try:
            # First try using backend_config (Config class instance)
            anomaly_detection_enabled = backend_config.is_anomaly_detection_enabled()
            print(f"   ℹ️  Read anomaly_detection.enabled from Config class: {anomaly_detection_enabled}")
        except Exception as e:
            print(f"   ⚠️  Could not check anomaly detection feature flag via Config class: {e}")
            # Try to read from config directly (YAML dict)
            try:
                anomaly_detection_enabled = config.get('features', {}).get('anomaly_detection', {}).get('enabled', False)
                print(f"   ℹ️  Read anomaly_detection.enabled from YAML dict: {anomaly_detection_enabled}")
            except Exception as e2:
                print(f"   ⚠️  Could not read from YAML dict: {e2}")
                pass
        
        # Final fallback: reload config file directly
        if not anomaly_detection_enabled:
            try:
                backend_config_path = Path(__file__).parent.parent / "backend" / "config.yaml"
                if backend_config_path.exists():
                    import yaml
                    with open(backend_config_path, 'r') as f:
                        fresh_config = yaml.safe_load(f)
                        anomaly_detection_enabled = fresh_config.get('features', {}).get('anomaly_detection', {}).get('enabled', False)
                        print(f"   ℹ️  Read anomaly_detection.enabled from fresh file read: {anomaly_detection_enabled}")
            except Exception as e3:
                print(f"   ⚠️  Could not read from fresh file: {e3}")
        
        if anomaly_detection_enabled:
            print("   ✅ Anomaly detection feature is enabled - will create car metrics topic")
            if 'car_metrics' in config['kafka']['topics']:
                topic_names.append(config['kafka']['topics']['car_metrics'])
            print("   ℹ️  Note: f1-car-metrics-anomalies topic will be created by Flink SQL statement")
        else:
            print("   ℹ️  Anomaly detection feature is disabled - skipping car metrics topics")
        
        # Step 6: Create Kafka Topics
        print("Step 6: Checking/Creating Kafka Topics...")
        if admin_kafka_config.get('bootstrap.servers') and admin_kafka_config.get('sasl.username') and admin_kafka_config.get('sasl.password'):
            print(f"   Using bootstrap servers: {admin_kafka_config.get('bootstrap.servers')}")
            print(f"   Using API key: {admin_kafka_config.get('sasl.username')}")
            kafka_admin = KafkaAdmin(admin_kafka_config)
            topics_created = kafka_admin.create_topics(topic_names)
            if not topics_created:
                print("⚠️  Some topics may not have been created. Continuing...")
        else:
            print("⚠️  Skipping topic creation - Kafka credentials not available")
            print("   Please ensure Kafka API keys were created in Step 5 or backend/config.yaml has bootstrap.servers, sasl.username, and sasl.password")
        print()
        
        # Step 7: Register Data Contracts (Schemas)
        print("Step 7: Registering Data Contracts (Schemas)...")
        if schema_registry_url and sr_api_key and sr_api_secret:
            try:
                schema_registry_config = {
                    'url': schema_registry_url,
                    'basic.auth.user.info': f"{sr_api_key}:{sr_api_secret}"
                }
                schema_admin = SchemaRegistryAdmin(schema_registry_config)
                
                # Register schema for f1-driver-positions topic
                positions_topic = config['kafka']['topics']['positions']
                positions_subject = f"{positions_topic}-value"
                schema_admin.register_schema(positions_subject, LEADERBOARD_UPDATE_SCHEMA, schema_type="AVRO")
                
                # Register schema for f1-car-metrics topic if anomaly detection is enabled
                if anomaly_detection_enabled and 'car_metrics' in config['kafka']['topics']:
                    car_metrics_topic = config['kafka']['topics']['car_metrics']
                    car_metrics_subject = f"{car_metrics_topic}-value"
                    schema_admin.register_schema(car_metrics_subject, CAR_METRICS_SCHEMA, schema_type="AVRO")
                
                print("✅ Data contracts registered successfully")
            except Exception as e:
                print(f"❌ Error registering data contracts: {e}")
        else:
            print("⚠️  Skipping data contract registration - Schema Registry credentials not available")
        print()
        
    except FileNotFoundError as e:
        print(f"⚠️  {e}")
        print("   Skipping topic creation. Please update backend/config.yaml and run again.")
        print("   Cloud resources (environment, cluster, Flink pool) are already created.")
    except KeyError as e:
        print(f"⚠️  Missing configuration key: {e}")
        print("   Please ensure backend/config.yaml has all required fields")
        print("   Cloud resources (environment, cluster, Flink pool) are already created.")
    except Exception as e:
        print(f"⚠️  Error setting up topics: {e}")
        print("   Cloud resources (environment, cluster, Flink pool) are already created.")
    
    print("=" * 60)
    print("✅ Infrastructure setup complete!")
    print("=" * 60)
    print()
    if kafka_api_key and kafka_api_secret:
        print("✅ API keys were created and used successfully!")
        print("   Topics have been set up.")
    else:
        print("⚠️  Some API keys could not be created automatically.")
        print("   Next steps:")
        if not (kafka_api_key and kafka_api_secret):
            print("   1. Create Kafka API keys for your cluster")
        if not (sr_api_key and sr_api_secret):
            print("   2. Create Schema Registry API keys (for backend configuration)")
        print("   3. Update backend/config.yaml with the credentials")
        print("   4. Re-run this script to create topics")
    print()


if __name__ == "__main__":
    main()

