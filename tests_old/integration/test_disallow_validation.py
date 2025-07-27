#!/usr/bin/env python3
"""
Test script to demonstrate DISALLOW validation with real examples.
"""

from processors.config_validator import validate_connector_config

def test_disallow_validation():
    """Test DISALLOW validation with realistic config scenarios."""
    
    print("Testing DISALLOW validation...")
    print("="*60)
    
    # Test 1: Valid source config (no disallowed fields)
    print("\n1. Testing valid source config (should pass):")
    valid_source_config = {
        'name': 'valid-source-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY',
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'topic.prefix': 'my-topic-prefix'
    }
    
    result = validate_connector_config(valid_source_config)
    print(f"  Valid: {result.is_valid}")
    print(f"  Disallowed present: {result.disallowed_present}")
    
    # Test 2: Source config with disallowed fields
    print("\n2. Testing source config with disallowed fields (should fail):")
    invalid_source_config = {
        'name': 'invalid-source-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY', 
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
        'topic.prefix': 'my-topic-prefix',
        # Add some disallowed fields
        'kafka.service.account.id': 'some-service-account',  # DISALLOW in general
        'topic.namespace.map': '{"db.coll": "custom-topic"}',  # DISALLOW in source
        'schema.context.name': 'custom-context',  # DISALLOW in general
        'offset.partition.name': 'custom-partition'  # DISALLOW in source
    }
    
    result = validate_connector_config(invalid_source_config)
    print(f"  Valid: {result.is_valid}")
    print(f"  Disallowed present: {result.disallowed_present}")
    if result.error_messages:
        print("  Error messages:")
        for error in result.error_messages:
            print(f"    - {error}")
    
    # Test 3: Sink config with disallowed fields
    print("\n3. Testing sink config with disallowed fields (should fail):")
    invalid_sink_config = {
        'name': 'invalid-sink-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY',
        'kafka.api.key': 'test-key', 
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'connector.class': 'com.mongodb.kafka.connect.MongoSinkConnector',
        'topics': 'test-topic',
        # Add some disallowed fields (from general configs)
        'header.converter': 'org.apache.kafka.connect.storage.StringConverter',
        'value.converter.use.latest.version': 'true',
        'key.converter.schemas.enable': 'false'
    }
    
    result = validate_connector_config(invalid_sink_config)
    print(f"  Valid: {result.is_valid}")
    print(f"  Disallowed present: {result.disallowed_present}")
    if result.error_messages:
        print("  Error messages:")
        for error in result.error_messages:
            print(f"    - {error}")
    
    print("\n" + "="*60)
    print("DISALLOW validation tests completed!")

if __name__ == '__main__':
    test_disallow_validation()