#!/usr/bin/env python3
"""
Kafka Topic Creator CLI

This script reads a main config file and processes connector configuration files
to create Kafka topics using the Confluent REST API.

Usage:
    python kafka_topic_creator.py <main_config.json> <connector_configs_folder>

Main config format:
{
    "confluent-cluster-id": "your-cluster-id",
    "confluent-rest-endpoint": "https://your-rest-endpoint.com"
}

Connector config format:
{
    "kafka.api.key": "your-api-key",
    "kafka.api.secret": "your-api-secret",
    "topic.prefix": "your-prefix",
    "database": "your-database",
    "collection": "your-collection",
    ...
}
"""

import json
import os
import sys
import requests
import argparse
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional


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
        "mongodb-cluster-name",
        "mongodb-connection-name"
    ]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in main config")
            return False
    
    return True


def validate_connector_config(config: Dict[str, Any], filename: str) -> bool:
    """Validate a connector configuration file."""
    required_fields = ["kafka.api.key", "kafka.api.secret", "topic.prefix", "database", "collection", "connection.user", "connection.password"]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in {filename}")
            return False
    
    return True


def create_mongodb_source_connection(
    group_id: str,
    tenant_name: str,
    cluster_name: str,
    connection_name: str,
    role_name: str = "readAnyDatabase",
    role_type: str = "BUILT_IN"
) -> tuple[bool, bool]:
    """Create a MongoDB Atlas Stream Processing source connection using Atlas CLI."""
    
    # Check if authenticated with Atlas CLI
    try:
        auth_check = subprocess.run(['atlas', 'auth', 'whoami'], capture_output=True, text=True, timeout=10)
        if auth_check.returncode != 0:
            print("✗ Not authenticated with Atlas CLI. Please run: atlas auth login")
            return False, False
    except Exception as e:
        print(f"✗ Error checking Atlas CLI authentication: {e}")
        return False, False
    
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
        
        # Create MongoDB source connection using Atlas CLI
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
            print(f"✓ Successfully created MongoDB source connection: {connection_name}")
            return True, True  # success, was_created
        else:
            # Check if connection already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ MongoDB source connection already exists, reusing: {connection_name}")
                return True, False  # success, was_created
            else:
                print(f"✗ Failed to create MongoDB source connection {connection_name}")
                print(f"  Error: {result.stderr}")
                return False, False  # success, was_created
                
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout creating MongoDB source connection {connection_name}")
        return False, False
    except Exception as e:
        print(f"✗ Unexpected error creating MongoDB source connection {connection_name}: {e}")
        return False, False
    finally:
        # Clean up temporary file
        if os.path.exists(temp_config_file):
            os.remove(temp_config_file)


def create_kafka_connection(
    stream_processor_url: str,
    group_id: str,
    tenant_name: str,
    connection_name: str,
    confluent_cluster_id: str,
    confluent_rest_endpoint: str,
    kafka_api_key: str,
    kafka_api_secret: str
) -> tuple[bool, bool]:
    """Create a MongoDB Atlas Stream Processing Kafka connection using Atlas CLI."""
    
    # Check if authenticated with Atlas CLI
    try:
        auth_check = subprocess.run(['atlas', 'auth', 'whoami'], capture_output=True, text=True, timeout=10)
        if auth_check.returncode != 0:
            print("✗ Not authenticated with Atlas CLI. Please run: atlas auth login")
            return False
    except Exception as e:
        print(f"✗ Error checking Atlas CLI authentication: {e}")
        return False
    
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
        if os.path.exists(temp_config_file):
            os.remove(temp_config_file)


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


def create_stream_processor(
    connection_user: str,
    connection_password: str,
    stream_processor_url: str,
    stream_processor_prefix: str,
    mongodb_source_connection_name: str,
    kafka_connection_name: str,
    database: str,
    collection: str,
    topic_prefix: str
) -> bool:
    """Create a stream processor using mongosh and sp.createStreamProcessor."""
    
    # Construct stream processor name
    stream_processor_name = f"{stream_processor_prefix}_{database}_{collection}"
    
    # Construct topic name
    topic_name = f"{topic_prefix}.{database}.{collection}"
    
    # Create the pipeline
    pipeline = [
        {
            "$source": {
                "connectionName": mongodb_source_connection_name,
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
    
    # Create JavaScript command for mongosh
    pipeline_json = json.dumps(pipeline)
    js_command = f'sp.createStreamProcessor("{stream_processor_name}", {pipeline_json})'
    
    # Ensure URL ends with exactly one slash
    if not stream_processor_url.endswith('/'):
        stream_processor_url += '/'
    
    # Build mongosh command (no quotes around URI when using subprocess)
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
        print(f"  Command: {' '.join(mongosh_cmd[:9])} --eval [JS_COMMAND]")
        print(f"  JS Command: {js_command}")
        result = subprocess.run(mongosh_cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            # Check if creation was successful or if it already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ Stream processor already exists: {stream_processor_name}")
                return True
            else:
                print(f"✓ Successfully created stream processor: {stream_processor_name}")
                return True
        else:
            # Check for already exists error in stderr
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ Stream processor already exists: {stream_processor_name}")
                return True
            else:
                print(f"✗ Failed to create stream processor {stream_processor_name}")
                print(f"  Error: {result.stderr}")
                return False
                
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout creating stream processor {stream_processor_name}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error creating stream processor {stream_processor_name}: {e}")
        return False


def extract_group_id_and_tenant_from_url(stream_processor_url: str) -> tuple:
    """Extract group ID and tenant name from stream processor instance URL."""
    # This is a helper function to parse the URL and extract required info
    # The actual implementation depends on the URL format MongoDB provides
    # For now, we'll assume these need to be provided separately in config
    return None, None


def process_connector_configs(main_config: Dict[str, Any], configs_folder: str) -> None:
    """Process all connector configuration files in the specified folder."""
    
    # Check Atlas CLI authentication first - stop all processing if not authenticated
    try:
        auth_check = subprocess.run(['atlas', 'auth', 'whoami'], capture_output=True, text=True, timeout=10)
        if auth_check.returncode != 0:
            print("✗ Not authenticated with Atlas CLI. Please run: atlas auth login")
            print("✗ All processing stopped due to authentication failure")
            return
    except Exception as e:
        print(f"✗ Error checking Atlas CLI authentication: {e}")
        print("✗ All processing stopped due to authentication failure")
        return
    
    folder_path = Path(configs_folder)
    
    if not folder_path.exists():
        print(f"Error: Folder not found: {configs_folder}")
        return
    
    if not folder_path.is_dir():
        print(f"Error: Path is not a directory: {configs_folder}")
        return
    
    # Find all .json files in the folder
    json_files = list(folder_path.glob("*.json"))
    
    if not json_files:
        print(f"No .json files found in {configs_folder}")
        return
    
    print(f"Found {len(json_files)} .json files to process")
    print("-" * 50)
    
    # Create connections once (using first connector config for Kafka auth)
    kafka_connection_created = False
    kafka_connection_was_created = False
    mongodb_connection_created = False
    mongodb_connection_was_created = False
    first_connector_config = None
    
    for json_file in json_files:
        connector_config = load_json_file(str(json_file))
        if connector_config and validate_connector_config(connector_config, json_file.name):
            first_connector_config = connector_config
            break
    
    # Create MongoDB source connection
    if "mongodb-group-id" in main_config and "mongodb-tenant-name" in main_config:
        print(f"\nCreating shared MongoDB source connection: {main_config['mongodb-connection-name']}")
        mongodb_connection_created, mongodb_connection_was_created = create_mongodb_source_connection(
            main_config["mongodb-group-id"],
            main_config["mongodb-tenant-name"],
            main_config["mongodb-cluster-name"],
            main_config["mongodb-connection-name"]
        )
    
    # Create Kafka connection
    if first_connector_config and "mongodb-group-id" in main_config and "mongodb-tenant-name" in main_config:
        print(f"\nCreating shared Kafka connection: {main_config['kafka-connection-name']}")
        kafka_connection_created, kafka_connection_was_created = create_kafka_connection(
            main_config["mongodb-stream-processor-instance-url"],
            main_config["mongodb-group-id"],
            main_config["mongodb-tenant-name"],
            main_config["kafka-connection-name"],
            main_config["confluent-cluster-id"],
            main_config["confluent-rest-endpoint"],
            first_connector_config["kafka.api.key"],
            first_connector_config["kafka.api.secret"]
        )
    
    kafka_success_count = 0
    stream_success_count = 0
    stream_processor_success_count = 0
    total_count = len(json_files)
    
    for json_file in json_files:
        print(f"\nProcessing: {json_file.name}")
        
        # Load connector config
        connector_config = load_json_file(str(json_file))
        if not connector_config:
            continue
        
        # Validate connector config
        if not validate_connector_config(connector_config, json_file.name):
            continue
        
        # Extract required fields
        api_key = connector_config["kafka.api.key"]
        api_secret = connector_config["kafka.api.secret"]
        topic_prefix = connector_config["topic.prefix"]
        database = connector_config["database"]
        collection = connector_config["collection"]
        connection_user = connector_config["connection.user"]
        connection_password = connector_config["connection.password"]
        
        # Construct topic name
        topic_name = f"{topic_prefix}.{database}.{collection}"
        
        # Create Kafka topic
        kafka_success = create_topic(
            main_config["confluent-rest-endpoint"],
            main_config["confluent-cluster-id"],
            api_key,
            api_secret,
            topic_name
        )
        
        if kafka_success:
            kafka_success_count += 1
            
            # Since Kafka connection is already created, we just report success
            if kafka_connection_created:
                stream_success_count += 1
                print(f"✓ Using existing Kafka connection: {main_config['kafka-connection-name']}")
                
                # Create stream processor if both connections exist
                if mongodb_connection_created:
                    stream_processor_success = create_stream_processor(
                        connection_user,
                        connection_password,
                        main_config["mongodb-stream-processor-instance-url"],
                        main_config["stream-processor-prefix"],
                        main_config["mongodb-connection-name"],
                        main_config["kafka-connection-name"],
                        database,
                        collection,
                        topic_prefix
                    )
                    
                    if stream_processor_success:
                        stream_processor_success_count += 1
                else:
                    print(f"⚠ Skipping stream processor creation: MongoDB source connection not available")
            else:
                print(f"⚠ Skipping stream connection creation: Kafka connection not available")
    
    print("-" * 50)
    print(f"Summary:")
    print(f"  Kafka topics: {kafka_success_count}/{total_count} created successfully")
    if mongodb_connection_created:
        connection_status = "created" if mongodb_connection_was_created else "reused (already existed)"
        print(f"  MongoDB source connection: 1/1 {connection_status}")
    else:
        print(f"  MongoDB source connection: 0/1 created successfully")
    if kafka_connection_created:
        connection_status = "created" if kafka_connection_was_created else "reused (already existed)"
        print(f"  Kafka connection: 1/1 {connection_status}")
    else:
        print(f"  Kafka connection: 0/1 created successfully")
    print(f"  Stream processors: {stream_processor_success_count}/{total_count} created successfully")


def main():
    """Main function to handle command-line arguments and orchestrate the process."""
    
    parser = argparse.ArgumentParser(
        description="Create Kafka topics from connector configuration files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "main_config",
        help="Path to the main configuration JSON file"
    )
    
    parser.add_argument(
        "configs_folder",
        help="Path to the folder containing connector configuration files"
    )
    
    args = parser.parse_args()
    
    # Load and validate main config
    print("Loading main configuration...")
    main_config = load_json_file(args.main_config)
    if not main_config:
        sys.exit(1)
    
    if not validate_main_config(main_config):
        sys.exit(1)
    
    print(f"✓ Main config loaded successfully")
    print(f"  Confluent Cluster ID: {main_config['confluent-cluster-id']}")
    print(f"  Confluent REST Endpoint: {main_config['confluent-rest-endpoint']}")
    print(f"  Stream Processor URL: {main_config['mongodb-stream-processor-instance-url']}")
    print(f"  Stream Processor Prefix: {main_config['stream-processor-prefix']}")
    print(f"  Kafka Connection Name: {main_config['kafka-connection-name']}")
    print(f"  MongoDB Source Cluster Name: {main_config['mongodb-cluster-name']}")
    print(f"  MongoDB Source Connection Name: {main_config['mongodb-connection-name']}")
    
    # Process connector configs
    process_connector_configs(main_config, args.configs_folder)


if __name__ == "__main__":
    main()