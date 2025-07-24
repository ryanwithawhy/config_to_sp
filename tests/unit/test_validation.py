#!/usr/bin/env python3
"""
Unit tests for configuration validation functionality.
"""

import unittest

# Add parent directory to path to import from common
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.common import validate_main_config


class TestValidateMainConfig(unittest.TestCase):
    """Test cases for validate_main_config function."""
    
    def test_valid_config(self):
        """Test validation with a complete, valid config."""
        valid_config = {
            "confluent-cluster-id": "test-cluster",
            "confluent-rest-endpoint": "https://test-endpoint.com",
            "mongodb-stream-processor-instance-url": "mongodb://test-url/",
            "stream-processor-prefix": "test-prefix",
            "kafka-connection-name": "test-kafka",
            "mongodb-connection-name": "test-mongodb",
            "mongodb-cluster-name": "TestCluster",
            "mongodb-group-id": "507f1f77bcf86cd799439011",
            "mongodb-tenant-name": "test-tenant"
        }
        
        result = validate_main_config(valid_config)
        self.assertTrue(result)
    
    def test_missing_required_field(self):
        """Test validation fails when required field is missing."""
        invalid_config = {
            "confluent-cluster-id": "test-cluster",
            "confluent-rest-endpoint": "https://test-endpoint.com",
            # Missing mongodb-stream-processor-instance-url
            "stream-processor-prefix": "test-prefix",
            "kafka-connection-name": "test-kafka",
            "mongodb-connection-name": "test-mongodb",
            "mongodb-cluster-name": "TestCluster",
            "mongodb-group-id": "507f1f77bcf86cd799439011",
            "mongodb-tenant-name": "test-tenant"
        }
        
        result = validate_main_config(invalid_config)
        self.assertFalse(result)
    
    def test_empty_config(self):
        """Test validation fails with empty config."""
        result = validate_main_config({})
        self.assertFalse(result)
    
    def test_extra_fields_allowed(self):
        """Test that extra fields don't break validation."""
        config_with_extras = {
            "confluent-cluster-id": "test-cluster",
            "confluent-rest-endpoint": "https://test-endpoint.com",
            "mongodb-stream-processor-instance-url": "mongodb://test-url/",
            "stream-processor-prefix": "test-prefix",
            "kafka-connection-name": "test-kafka",
            "mongodb-connection-name": "test-mongodb",
            "mongodb-cluster-name": "TestCluster",
            "mongodb-group-id": "507f1f77bcf86cd799439011",
            "mongodb-tenant-name": "test-tenant",
            "extra-field": "extra-value",  # Extra field
            "another-extra": 123
        }
        
        result = validate_main_config(config_with_extras)
        self.assertTrue(result)
    
    def test_all_required_fields_present(self):
        """Test that all expected required fields are checked."""
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
        
        # Test each field individually by removing it
        for field_to_remove in required_fields:
            config = {field: "test-value" for field in required_fields}
            del config[field_to_remove]
            
            result = validate_main_config(config)
            self.assertFalse(result, f"Validation should fail when {field_to_remove} is missing")


if __name__ == '__main__':
    unittest.main()