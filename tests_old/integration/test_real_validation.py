#!/usr/bin/env python3
"""
Test script to validate the config validator against real CSV data.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.config_validator import validate_connector_config

def test_real_csv_loading():
    """Test loading real CSV files and validating sample configs."""
    
    print("Testing real CSV file loading...")
    
    # Test with a sample source connector config
    source_config = {
        'name': 'test-source-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY',
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector'
    }
    
    try:
        result = validate_connector_config(source_config)  # Auto-detect from connector.class
        print(f"Source validation - Valid: {result.is_valid}")
        
        if not result.is_valid:
            print("Validation errors:")
            for error in result.error_messages:
                print(f"  - {error}")
        
        print(f"Missing required: {result.missing_required}")
        print(f"Disallowed present: {result.disallowed_present}")
        
    except Exception as e:
        print(f"Error during source validation: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # Test with a sample sink connector config
    sink_config = {
        'name': 'test-sink-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY',
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'connector.class': 'com.mongodb.kafka.connect.MongoSinkConnector',
        'topics': 'test-topic'
    }
    
    try:
        result = validate_connector_config(sink_config)  # Auto-detect from connector.class
        print(f"Sink validation - Valid: {result.is_valid}")
        
        if not result.is_valid:
            print("Validation errors:")
            for error in result.error_messages:
                print(f"  - {error}")
        
        print(f"Missing required: {result.missing_required}")
        print(f"Disallowed present: {result.disallowed_present}")
        
    except Exception as e:
        print(f"Error during sink validation: {e}")


if __name__ == '__main__':
    test_real_csv_loading()