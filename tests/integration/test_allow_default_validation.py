#!/usr/bin/env python3
"""
Test ALLOW DEFAULT validation behavior.
For ALLOW DEFAULT fields:
- If field is absent: Treat like IGNORE (pass)
- If field equals default value: Treat like IGNORE (pass)  
- If field has different value: Error with message "Only {default} is supported for {field}"
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.config_validator import validate_connector_config

def test_allow_default_validation():
    """Test ALLOW DEFAULT validation with real examples."""
    
    print("Testing ALLOW DEFAULT validation...")
    print("="*60)
    
    # Test 1: Field absent (should pass - treated like IGNORE)
    print("\n1. Testing with ALLOW DEFAULT field absent (should pass):")
    config_field_absent = {
        'name': 'test-source-connector',
        # kafka.auth.mode is ALLOW DEFAULT with default "KAFKA_API_KEY" - testing without it
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
    
    result = validate_connector_config(config_field_absent)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 2: Field present with correct default value (should pass)
    print("\n2. Testing with ALLOW DEFAULT field set to correct default (should pass):")
    config_correct_default = config_field_absent.copy()
    config_correct_default['kafka.auth.mode'] = 'KAFKA_API_KEY'  # Correct default
    
    result = validate_connector_config(config_correct_default)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 3: Field present with incorrect value (should fail)
    print("\n3. Testing with ALLOW DEFAULT field set to incorrect value (should fail):")
    config_incorrect_value = config_field_absent.copy()
    config_incorrect_value['kafka.auth.mode'] = 'SERVICE_ACCOUNT'  # Wrong value
    
    result = validate_connector_config(config_incorrect_value)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 4: Multiple ALLOW DEFAULT fields with sink config
    print("\n4. Testing sink config with multiple ALLOW DEFAULT fields:")
    sink_config = {
        'name': 'test-sink-connector',
        'kafka.auth.mode': 'KAFKA_API_KEY',  # Correct default
        'kafka.api.key': 'test-key',
        'kafka.api.secret': 'test-secret',
        'connection.host': 'test.mongodb.net',
        'connection.user': 'testuser',
        'connection.password': 'testpass',
        'database': 'testdb',
        'collection': 'testcoll',
        'connector.class': 'com.mongodb.kafka.connect.MongoSinkConnector',
        'topics': 'test-topic',
        'input.data.format': 'JSON',  # Correct default
        'input.key.format': 'STRING',  # Correct default
        'delete.on.null.values': 'FALSE'  # Correct default
    }
    
    result = validate_connector_config(sink_config)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 5: Sink config with wrong ALLOW DEFAULT values
    print("\n5. Testing sink config with wrong ALLOW DEFAULT values (should fail):")
    sink_config_wrong = sink_config.copy()
    sink_config_wrong['input.data.format'] = 'AVRO'  # Wrong (default is JSON)
    sink_config_wrong['delete.on.null.values'] = 'TRUE'  # Wrong (default is FALSE)
    
    result = validate_connector_config(sink_config_wrong)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages:")
    for error in result.error_messages:
        print(f"    - {error}")
    
    print("\n" + "="*60)
    print("ALLOW DEFAULT validation tests completed!")

if __name__ == '__main__':
    test_allow_default_validation()