#!/usr/bin/env python3
"""
MongoDB Sink Stream Processor Setup Tool

This script reads a main config file and processes connector configuration files
to create MongoDB sink stream processors using MongoDB Atlas Stream Processing.

Usage:
    python create_sink_processors.py <main_config.json> <connector_configs_folder>

Main config format:
{
    "confluent-cluster-id": "your-cluster-id",
    "confluent-rest-endpoint": "https://your-rest-endpoint.com",
    "mongodb-stream-processor-instance-url": "mongodb://your-stream-processor-instance-url/",
    "stream-processor-prefix": "managed-sink-test",
    "kafka-connection-name": "kafka-connection-name",
    "mongodb-connection-name": "mongodb-sink-connection",
    "mongodb-group-id": "your-24-char-mongodb-group-id",
    "mongodb-tenant-name": "your-stream-instance-name"
}

Connector config format:
{
    "kafka.api.key": "your-api-key",
    "kafka.api.secret": "your-api-secret",
    "input.data.format": "JSON",
    "connection.user": "your-username",
    "connection.password": "your-password",
    "topics": "topic-name" || ["topic1", "topic2"],
    "database": "your-database",
    "collection": "your-collection"
}
"""

import json
import os
import sys
import requests
import argparse
import subprocess
from pathlib import Path
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
        "mongodb-tenant-name"
    ]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in main config")
            return False
    
    return True


def validate_connector_config(config: Dict[str, Any], filename: str) -> bool:
    """Validate a connector configuration file."""
    required_fields = [
        "kafka.api.key", 
        "kafka.api.secret", 
        "input.data.format",
        "connection.user", 
        "connection.password",
        "topics",
        "database", 
        "collection"
    ]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in {filename}")
            return False
    
    # Validate topics field (can be string or array)
    topics = config.get("topics")
    if not isinstance(topics, (str, list)):
        print(f"Error: 'topics' field must be a string or array in {filename}")
        return False
    
    if isinstance(topics, list) and len(topics) == 0:
        print(f"Error: 'topics' array cannot be empty in {filename}")
        return False
    
    return True


def create_mongodb_sink_connection(
    group_id: str,
    tenant_name: str,
    cluster_name: str,
    connection_name: str,
    role_name: str = "readWriteAnyDatabase",
    role_type: str = "BUILT_IN"
) -> tuple[bool, bool]:
    """Create a MongoDB Atlas Stream Processing sink connection using Atlas CLI."""
    
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
        
        # Create MongoDB sink connection using Atlas CLI
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
            print(f"✓ Successfully created MongoDB sink connection: {connection_name}")
            return True, True  # success, was_created
        else:
            # Check if connection already exists
            if "already exists" in result.stderr.lower() or "duplicate" in result.stderr.lower():
                print(f"⚠ MongoDB sink connection already exists, reusing: {connection_name}")
                return True, False  # success, was_created
            else:
                print(f"✗ Failed to create MongoDB sink connection {connection_name}")
                print(f"  Error: {result.stderr}")
                return False, False  # success, was_created
                
    except subprocess.TimeoutExpired:
        print(f"✗ Timeout creating MongoDB sink connection {connection_name}")
        return False, False
    except Exception as e:
        print(f"✗ Unexpected error creating MongoDB sink connection {connection_name}: {e}")
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
            return False, False
    except Exception as e:
        print(f"✗ Error checking Atlas CLI authentication: {e}")
        return False, False
    
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




def create_stream_processor(
    connection_user: str,
    connection_password: str,
    stream_processor_url: str,
    stream_processor_prefix: str,
    kafka_connection_name: str,
    mongodb_connection_name: str,
    database: str,
    collection: str,
    topics: Union[str, List[str]],
    auto_offset_reset: Optional[str] = None
) -> bool:
    """Create a stream processor using mongosh and sp.createStreamProcessor."""
    
    # Construct stream processor name
    stream_processor_name = f"{stream_processor_prefix}_{database}_{collection}"
    
    # Create the $source stage
    source_stage = {
        "connectionName": kafka_connection_name,
        "topic": topics
    }
    
    # Add config with auto_offset_reset if provided
    if auto_offset_reset:
        source_stage["config"] = {
            "auto_offset_reset": auto_offset_reset
        }
    
    # Create the pipeline with $source and $merge
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
    
    # Create MongoDB sink connection
    if "mongodb-group-id" in main_config and "mongodb-tenant-name" in main_config:
        print(f"\nCreating MongoDB sink connection: {main_config['mongodb-connection-name']}")
        mongodb_connection_created, mongodb_connection_was_created = create_mongodb_sink_connection(
            main_config["mongodb-group-id"],
            main_config["mongodb-tenant-name"],
            main_config["mongodb-cluster-name"],
            main_config["mongodb-connection-name"]
        )
    
    # Create Kafka connection
    if first_connector_config and "mongodb-group-id" in main_config and "mongodb-tenant-name" in main_config:
        print(f"\nCreating Kafka connection: {main_config['kafka-connection-name']}")
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
        database = connector_config["database"]
        collection = connector_config["collection"]
        topics = connector_config["topics"]
        connection_user = connector_config["connection.user"]
        connection_password = connector_config["connection.password"]
        
        # Handle optional auto offset reset
        auto_offset_reset = connector_config.get("consumer.override.auto.offset.reset")
        if auto_offset_reset and auto_offset_reset not in ["earliest", "latest"]:
            print(f"✗ Error: auto offset reset must be 'earliest' or 'latest', got '{auto_offset_reset}' in {json_file.name}")
            continue
        
        
        # Create stream processor if both connections exist
        if mongodb_connection_created and kafka_connection_created:
            stream_processor_success = create_stream_processor(
                connection_user,
                connection_password,
                main_config["mongodb-stream-processor-instance-url"],
                main_config["stream-processor-prefix"],
                main_config["kafka-connection-name"],
                main_config["mongodb-connection-name"],
                database,
                collection,
                topics,
                auto_offset_reset
            )
            
            if stream_processor_success:
                stream_processor_success_count += 1
        else:
            print(f"⚠ Skipping stream processor creation: Required connections not available")
    
    print("-" * 50)
    print(f"Summary:")
    if mongodb_connection_created:
        connection_status = "created" if mongodb_connection_was_created else "reused (already existed)"
        print(f"  MongoDB sink connection: 1/1 {connection_status}")
    else:
        print(f"  MongoDB sink connection: 0/1 created successfully")
    if kafka_connection_created:
        connection_status = "created" if kafka_connection_was_created else "reused (already existed)"
        print(f"  Kafka connection: 1/1 {connection_status}")
    else:
        print(f"  Kafka connection: 0/1 created successfully")
    print(f"  Stream processors: {stream_processor_success_count}/{total_count} created successfully")


def main():
    """Main function to handle command-line arguments and orchestrate the process."""
    
    parser = argparse.ArgumentParser(
        description="Create MongoDB sink stream processors from connector configuration files",
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
    print(f"  MongoDB Connection Name: {main_config['mongodb-connection-name']}")
    
    # Process connector configs
    process_connector_configs(main_config, args.configs_folder)


if __name__ == "__main__":
    main()