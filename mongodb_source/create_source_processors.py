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
    required_fields = ["confluent-cluster-id", "confluent-rest-endpoint"]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in main config")
            return False
    
    return True


def validate_connector_config(config: Dict[str, Any], filename: str) -> bool:
    """Validate a connector configuration file."""
    required_fields = ["kafka.api.key", "kafka.api.secret", "topic.prefix", "database", "collection"]
    
    for field in required_fields:
        if field not in config:
            print(f"Error: Missing required field '{field}' in {filename}")
            return False
    
    return True


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
            print(f"✗ Failed to create topic {topic_name}: HTTP {response.status_code}")
            print(f"  Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Network error creating topic {topic_name}: {e}")
        return False
    except Exception as e:
        print(f"✗ Unexpected error creating topic {topic_name}: {e}")
        return False


def process_connector_configs(main_config: Dict[str, Any], configs_folder: str) -> None:
    """Process all connector configuration files in the specified folder."""
    
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
    
    success_count = 0
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
        
        # Construct topic name
        topic_name = f"{topic_prefix}.{database}.{collection}"
        
        # Create topic
        if create_topic(
            main_config["confluent-rest-endpoint"],
            main_config["confluent-cluster-id"],
            api_key,
            api_secret,
            topic_name
        ):
            success_count += 1
    
    print("-" * 50)
    print(f"Summary: {success_count}/{total_count} topics processed successfully")


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
    print(f"  Cluster ID: {main_config['confluent-cluster-id']}")
    print(f"  REST Endpoint: {main_config['confluent-rest-endpoint']}")
    
    # Process connector configs
    process_connector_configs(main_config, args.configs_folder)


if __name__ == "__main__":
    main()