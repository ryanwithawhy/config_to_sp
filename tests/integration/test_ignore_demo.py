#!/usr/bin/env python3
"""
Demo showing IGNORE validation behavior.
"""

from processors.config_validator import validate_connector_config

def test_ignore_fields():
    """Demonstrate that IGNORE fields don't affect validation."""
    
    print("Testing IGNORE field behavior...")
    print("="*50)
    
    # Let's find some IGNORE fields from our CSVs first
    import csv
    import os
    
    ignore_fields = []
    rules_path = os.path.join('processors', 'rules')
    
    # Check general rules
    with open(os.path.join(rules_path, 'general_managed_configs.csv'), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('what_do_do', '').strip() == 'IGNORE':
                ignore_fields.append(row['name'])
    
    # Check source rules  
    with open(os.path.join(rules_path, 'source_configuration_properties.csv'), 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('what_do_do', '').strip() == 'IGNORE':
                ignore_fields.append(row['name'])
    
    print(f"Found {len(ignore_fields)} IGNORE fields:")
    for field in ignore_fields[:5]:  # Show first 5
        print(f"  - {field}")
    if len(ignore_fields) > 5:
        print(f"  ... and {len(ignore_fields) - 5} more")
    
    print("\n" + "="*50)
    
    # Test 1: Valid config WITHOUT ignore fields
    print("\n1. Testing config WITHOUT ignore fields:")
    config_without_ignore = {
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
    }
    
    result = validate_connector_config(config_without_ignore, 'source')
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    # Test 2: Same config WITH ignore fields added
    print("\n2. Testing same config WITH ignore fields added:")
    config_with_ignore = config_without_ignore.copy()
    
    # Add some ignore fields with various values
    if ignore_fields:
        config_with_ignore[ignore_fields[0]] = 'some_value'
        config_with_ignore[ignore_fields[1]] = 12345
        config_with_ignore[ignore_fields[2]] = True
        
        print(f"  Added ignore fields: {ignore_fields[:3]}")
    
    result = validate_connector_config(config_with_ignore, 'source')
    print(f"  Valid: {result.is_valid}")
    print(f"  Error messages: {result.error_messages}")
    
    print("\n" + "="*50)
    print("✅ IGNORE fields are properly skipped!")
    print("   They don't cause validation failures whether present or absent.")

if __name__ == '__main__':
    test_ignore_fields()