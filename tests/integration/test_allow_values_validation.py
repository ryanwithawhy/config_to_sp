#!/usr/bin/env python3
"""
Test ALLOW {values} validation behavior.
For ALLOW {values} fields:
- If field is absent: Pass (treat like IGNORE)
- If field equals one of the allowed values: Pass  
- If field has different value: Error with message "Only {value1}, {value2} is supported for {field}"
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.config_validator import validate_connector_config

def test_allow_values_validation():
    """Test ALLOW {values} validation with real examples."""
    
    print("Testing ALLOW {values} validation...")
    print("="*60)
    
    # Test 1: Field absent (should pass - treated like IGNORE)
    print("\n1. Testing with ALLOW {values} field absent (should pass):")
    config_field_absent = {
        'name': 'test-source-connector',
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
        # output.data.format is ALLOW JSON - testing without it
    }
    
    result = validate_connector_config(config_field_absent)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 2: Field present with allowed value (should pass)
    print("\n2. Testing with ALLOW {values} field set to allowed value (should pass):")
    config_allowed_value = config_field_absent.copy()
    config_allowed_value['output.data.format'] = 'JSON'  # Allowed value
    
    result = validate_connector_config(config_allowed_value)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 3: Field present with disallowed value (should fail)
    print("\n3. Testing with ALLOW {values} field set to disallowed value (should fail):")
    config_disallowed_value = config_field_absent.copy()
    config_disallowed_value['output.data.format'] = 'AVRO'  # Not allowed
    
    result = validate_connector_config(config_disallowed_value)
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 4: Test startup.mode with multiple allowed values
    print("\n4. Testing startup.mode with multiple allowed values:")
    config_startup_latest = config_field_absent.copy()
    config_startup_latest['startup.mode'] = 'latest'  # One of: latest, copy_existing
    
    result = validate_connector_config(config_startup_latest)
    print(f"  startup.mode=latest - Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    config_startup_copy = config_field_absent.copy()
    config_startup_copy['startup.mode'] = 'copy_existing'  # One of: latest, copy_existing
    
    result = validate_connector_config(config_startup_copy)
    print(f"  startup.mode=copy_existing - Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    config_startup_invalid = config_field_absent.copy()
    config_startup_invalid['startup.mode'] = 'earliest'  # Not allowed
    
    result = validate_connector_config(config_startup_invalid)
    print(f"  startup.mode=earliest - Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    print("\n" + "="*60)
    print("ALLOW {values} validation tests completed!")

if __name__ == '__main__':
    test_allow_values_validation()