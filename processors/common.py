#!/usr/bin/env python3
"""
Common utility functions shared between source and sink processor creation scripts.
"""

import json
import subprocess
import sys
import requests
from typing import Dict, Any, Optional, Union, List


def load_json_file(file_path: str) -> Optional[Dict[str, Any]]:
    """Load and parse a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path}: {e}")
        return None
    except Exception as e:
        print(f"Error: Failed to read {file_path}: {e}")
        return None


def validate_main_config(config: Dict[str, Any]) -> bool:
    """Validate the main configuration file."""
    required_fields = [
        "confluent-cluster-id", 
        "confluent-rest-endpoint",
        "mongodb-stream-processor-instance-url",
        "stream-processor-prefix",
        "kafka-connection-name",
        "mongodb-connection-name",
        "mongodb-cluster-name",
        "mongodb-group-id",
        "mongodb-tenant-name",
        "mongodb-connection-role"
    ]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in main config")
            return False
    
    return True


def check_atlas_auth_with_login() -> bool:
    """
    Check if authenticated with Atlas CLI and prompt for login if not authenticated.
    Returns True if authenticated (or becomes authenticated), False if user declines login.
    """
    try:
        # Check current authentication status
        auth_check = subprocess.run(['atlas', 'auth', 'whoami'], capture_output=True, text=True, timeout=10)
        if auth_check.returncode == 0:
            print("✓ Already authenticated with Atlas CLI")
            return True
    except Exception as e:
        print(f"✗ Error checking Atlas CLI authentication: {e}")
        return False
    
    # Not authenticated - prompt user for login
    print("✗ Not authenticated with Atlas CLI")
    
    try:
        # Prompt user with default yes
        response = input("Would you like to login now? [Y/n]: ").strip().lower()
        
        # Default to 'yes' if empty response
        if response == '' or response == 'y' or response == 'yes':
            print("Running: atlas auth login")
            try:
                # Run atlas auth login interactively
                login_result = subprocess.run(['atlas', 'auth', 'login'], timeout=120)
                
                if login_result.returncode == 0:
                    print("✓ Successfully authenticated with Atlas CLI")
                    return True
                else:
                    print("✗ Failed to authenticate with Atlas CLI")
                    return False
                    
            except subprocess.TimeoutExpired:
                print("✗ Login process timed out")
                return False
            except Exception as e:
                print(f"✗ Error during login: {e}")
                return False
        else:
            print("✗ Cannot proceed without Atlas CLI authentication")
            print("  Please run 'atlas auth login' manually and try again")
            return False
            
    except KeyboardInterrupt:
        print("\n✗ Login cancelled by user")
        return False
    except Exception as e:
        print(f"✗ Error during login prompt: {e}")
        return False


def create_mongodb_connection(
    group_id: str,
    tenant_name: str,
    cluster_name: str,
    connection_name: str,
    role_name: str,
    role_type: str = "BUILT_IN"
) -> tuple[bool, bool]:
    """Create a MongoDB Atlas Stream Processing connection using Atlas CLI."""
    
    # Create connection configuration
    connection_config = {
        "type": "Cluster",
        "clusterName": cluster_name,
        "dbRoleToExecute": {
            "role": role_name,
            "type": role_type
        }
    }
    
    # Write temporary config file
    temp_config_file = "temporary-connection-file.json"
    try:
        with open(temp_config_file, 'w') as f:
            json.dump(connection_config, f, indent=2)
        
        # Create MongoDB connection using Atlas CLI
        cmd = [
            'atlas', 'streams', 'connections', 'create',
            connection_name,
            '--projectId', group_id,
            '--instance', tenant_name,
            '--file', temp_config_file,
            '--output', 'json'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✓ Successfully created MongoDB connection: {connection_name}")
            return True, True  # success, was_created
        else:
            # Check if connection already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ MongoDB connection already exists, reusing: {connection_name}")
                return True, False  # success, was_created
            else:
                print(f"✗ Failed to create MongoDB connection {connection_name}")
                print(f"  Error: {result.stderr}")
                return False, False  # success, was_created
                
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout creating MongoDB connection {connection_name}")
        return False, False
    except Exception as e:
        print(f"✗ Unexpected error creating MongoDB connection {connection_name}: {e}")
        return False, False
    finally:
        # Clean up temporary file
        import os
        if os.path.exists(temp_config_file):
            os.remove(temp_config_file)


def create_kafka_connection(
    group_id: str,
    tenant_name: str,
    connection_name: str,
    confluent_rest_endpoint: str,
    kafka_api_key: str,
    kafka_api_secret: str
) -> tuple[bool, bool]:
    """Create a MongoDB Atlas Stream Processing Kafka connection using Atlas CLI."""
    
    # Convert bootstrap servers from REST endpoint
    bootstrap_servers = confluent_rest_endpoint.replace('https://', '').replace(':443', ':9092')
    
    # Create connection configuration
    connection_config = {
        "name": connection_name,
        "type": "Kafka",
        "authentication": {
            "mechanism": "PLAIN",
            "username": kafka_api_key,
            "password": kafka_api_secret
        },
        "bootstrapServers": bootstrap_servers,
        "config": {
            "auto.offset.reset": "earliest",
            "group.id": f"{connection_name}-consumer-group"
        },
        "security": {
            "protocol": "SASL_SSL"
        }
    }
    
    # Write temporary config file
    temp_config_file = f"/tmp/{connection_name}_config.json"
    try:
        with open(temp_config_file, 'w') as f:
            json.dump(connection_config, f, indent=2)
        
        # Create Kafka connection using Atlas CLI
        cmd = [
            'atlas', 'streams', 'connection', 'create',
            connection_name,
            '--projectId', group_id,
            '--instance', tenant_name,
            '--file', temp_config_file,
            '--output', 'json'
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print(f"✓ Successfully created Kafka connection: {connection_name}")
            return True, True  # success, was_created
        else:
            # Check if connection already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ Kafka connection already exists, reusing: {connection_name}")
                return True, False  # success, was_created
            else:
                print(f"✗ Failed to create Kafka connection {connection_name}")
                print(f"  Error: {result.stderr}")
                return False, False  # success, was_created
                
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout creating Kafka connection {connection_name}")
        return False, False
    except Exception as e:
        print(f"✗ Unexpected error creating Kafka connection {connection_name}: {e}")
        return False, False
    finally:
        # Clean up temporary file
        import os
        if os.path.exists(temp_config_file):
            os.remove(temp_config_file)


def create_stream_processor(
    connection_user: str,
    connection_password: str,
    stream_processor_url: str,
    kafka_connection_name: str,
    mongodb_connection_name: str,
    database: str,
    collection: str,
    processor_type: str,
    processor_name: str,
    topic_prefix: Optional[str] = None,
    topics: Optional[Union[str, List[str]]] = None,
    auto_offset_reset: Optional[str] = None
) -> bool:
    """
    Create a stream processor using mongosh and sp.createStreamProcessor.
    
    Args:
        connection_user: MongoDB user for authentication
        connection_password: MongoDB password for authentication  
        stream_processor_url: MongoDB stream processor instance URL
        kafka_connection_name: Name of the Kafka connection
        mongodb_connection_name: Name of the MongoDB connection
        database: Database name
        collection: Collection name
        processor_type: Type of processor ('source' or 'sink')
        processor_name: Name of the stream processor from config file
        topic_prefix: Topic prefix for source processors (required for source)
        topics: Topics for sink processors (required for sink)
        auto_offset_reset: Auto offset reset strategy for sink processors
        
    Returns:
        tuple: (success: bool, was_created: bool, processor_name: str)
               success: True if stream processor was created or already exists, False on error
               was_created: True if newly created, False if already existed
               processor_name: Name of the stream processor
    """
    
    # Use the provided processor name from config
    stream_processor_name = processor_name
    
    # Create pipeline based on processor type
    if processor_type == "source":
        if not topic_prefix:
            print(f"✗ Error: topic_prefix is required for source processors")
            return False
            
        # Construct topic name
        topic_name = f"{topic_prefix}.{database}.{collection}"
        
        # Create source pipeline with $source (MongoDB) -> $emit (Kafka)
        pipeline = [
            {
                "$source": {
                    "connectionName": mongodb_connection_name,
                    "db": database,
                    "coll": collection
                }
            },
            {
                "$emit": {
                    "connectionName": kafka_connection_name,
                    "topic": topic_name
                }
            }
        ]
        
    elif processor_type == "sink":
        if not topics:
            print(f"✗ Error: topics is required for sink processors")
            return False
            
        # Create the $source stage for Kafka input
        source_stage = {
            "connectionName": kafka_connection_name,
            "topic": topics
        }
        
        # Add config with auto_offset_reset if provided
        if auto_offset_reset:
            source_stage["config"] = {
                "auto_offset_reset": auto_offset_reset
            }
        
        # Create sink pipeline with $source (Kafka) -> $merge (MongoDB)
        pipeline = [
            {
                "$source": source_stage
            },
            {
                "$merge": {
                    "into": {
                        "connectionName": mongodb_connection_name,
                        "db": database,
                        "coll": collection
                    }
                }
            }
        ]
        
    else:
        print(f"✗ Error: Invalid processor_type '{processor_type}'. Must be 'source' or 'sink'")
        return False
    
    # Create JavaScript command for mongosh
    pipeline_json = json.dumps(pipeline)
    js_command = f'sp.createStreamProcessor("{stream_processor_name}", {pipeline_json})'
    
    # Ensure URL ends with exactly one slash
    if not stream_processor_url.endswith('/'):
        stream_processor_url += '/'
    
    # Build mongosh command
    mongosh_cmd = [
        'mongosh',
        stream_processor_url,
        '--tls',
        '--authenticationDatabase', 'admin',
        '--username', connection_user,
        '--password', connection_password,
        '--eval', js_command
    ]
    
    try:
        print(f"Creating stream processor: {stream_processor_name}")
        result = subprocess.run(mongosh_cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Check if creation was successful or if it already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ Stream processor already exists: {stream_processor_name}")
                return True, False, stream_processor_name  # success, not newly created
            else:
                print(f"✓ Successfully created stream processor: {stream_processor_name}")
                return True, True, stream_processor_name  # success, newly created
        else:
            # Check for already exists error in stderr
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ Stream processor already exists: {stream_processor_name}")
                return True, False, stream_processor_name  # success, not newly created
            else:
                print(f"✗ Failed to create stream processor {stream_processor_name}")
                print(f"  Error: {result.stderr}")
                return False, False, stream_processor_name  # failure
                
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout creating stream processor {stream_processor_name}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error creating stream processor {stream_processor_name}: {e}")
        return False


def create_topic(
    rest_endpoint: str,
    cluster_id: str,
    api_key: str,
    api_secret: str,
    topic_name: str
) -> bool:
    """Create a Kafka topic using the Confluent REST API."""
    
    url = f"{rest_endpoint}/kafka/v3/clusters/{cluster_id}/topics"
    
    headers = {
        "Content-Type": "application/json"
    }
    
    payload = {
        "topic_name": topic_name,
        "partitions_count": 3,
        "configs": [
            {"name": "cleanup.policy", "value": "delete"}
        ]
    }
    
    try:
        response = requests.post(
            url,
            auth=(api_key, api_secret),
            headers=headers,
            json=payload,
            timeout=30
        )
        
        if response.status_code == 201:
            print(f"✓ Successfully created topic: {topic_name}")
            return True
        elif response.status_code == 409:
            print(f"⚠ Topic already exists: {topic_name}")
            return True
        else:
            # Check if the error is specifically error code 40002
            try:
                response_json = response.json()
                error_code = response_json.get('error_code')
                if error_code == 40002:
                    print(f"ℹ Topic {topic_name} is already created")
                    return True
            except (json.JSONDecodeError, KeyError):
                pass
            
            print(f"✗ Failed to create topic {topic_name}: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating topic {topic_name}: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error creating topic {topic_name}: {e}")
        return False