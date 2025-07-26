#!/usr/bin/env python3
"""
Test auto-detection of connector type from connector.class field.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.config_validator import validate_connector_config

def test_auto_detection():
    """Test auto-detection of connector type."""
    
    print("Testing auto-detection of connector type...")
    print("="*60)
    
    # Test 1: Source connector auto-detection
    print("\n1. Testing Source connector auto-detection:")
    source_config = {
        'name': 'test-source-connector',
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'kafka.auth.mode': 'KAFKA_API_KEY',
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'topic.prefix': 'my-topic-prefix'
    }
    
    # Auto-detect (no connector_type specified)
    result = validate_connector_config(source_config)
    print(f"  Auto-detected as source - Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 2: Sink connector auto-detection  
    print("\n2. Testing Sink connector auto-detection:")
    sink_config = {
        'name': 'test-sink-connector',
        'connector.class': 'com.mongodb.kafka.connect.MongoSinkConnector',
        'kafka.auth.mode': 'KAFKA_API_KEY',
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'topics': 'test-topic'
    }
    
    # Auto-detect (no connector_type specified)
    result = validate_connector_config(sink_config)
    print(f"  Auto-detected as sink - Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 3: Invalid connector.class (should fail)
    print("\n3. Testing invalid connector.class (should fail):")
    invalid_config = {
        'name': 'test-invalid-connector',
        'connector.class': 'some.invalid.Connector',
        'kafka.auth.mode': 'KAFKA_API_KEY'
    }
    
    try:
        result = validate_connector_config(invalid_config)
        print(f"  Unexpected success: {result.is_valid}")
    except ValueError as e:
        print(f"  Expected error: {e}")
    
    # Test 4: Missing connector.class (should fail)
    print("\n4. Testing missing connector.class (should fail):")
    missing_config = {
        'name': 'test-missing-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY'
    }
    
    try:
        result = validate_connector_config(missing_config)
        print(f"  Unexpected success: {result.is_valid}")
    except ValueError as e:
        print(f"  Expected error: {e}")
    
    # Test 5: Manual override still works
    print("\n5. Testing manual connector_type override:")
    result_manual = validate_connector_config(source_config, connector_type='source')
    print(f"  Manual override - Valid: {result_manual.is_valid}")
    print(f"  Error messages: {result_manual.error_messages}")
    
    # Test 6: Source config with source-specific validation
    print("\n6. Testing source-specific field validation:")
    source_with_source_field = source_config.copy()
    source_with_source_field['topic.prefix'] = 'my-prefix'  # Required for source
    
    result = validate_connector_config(source_with_source_field)
    print(f"  Source with topic.prefix - Valid: {result.is_valid}")
    
    # Test 7: Sink config with sink-specific validation
    print("\n7. Testing sink-specific field validation:")
    sink_with_sink_field = sink_config.copy()
    sink_with_sink_field['topics'] = 'test-topic'  # Required for sink
    
    result = validate_connector_config(sink_with_sink_field)
    print(f"  Sink with topics - Valid: {result.is_valid}")
    
    print("\n" + "="*60)
    print("Auto-detection tests completed!")

if __name__ == '__main__':
    test_auto_detection()