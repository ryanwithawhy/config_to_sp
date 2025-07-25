#!/usr/bin/env python3
"""
Unit tests for unified connector configuration validation functionality.
"""

import unittest

# Add parent directory to path to import from create_processors
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from create_processors import validate_unified_config


class TestUnifiedValidation(unittest.TestCase):
    """Test cases for unified connector config validation."""
    
    def test_valid_source_config(self):
        """Test validation with a valid source config."""
        valid_config = {
            "connector.class": "MongoDbAtlasSource",
            "name": "test-source-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret"
        }
        
        result = validate_unified_config(valid_config, "test.json")
        self.assertTrue(result)
    
    def test_valid_sink_config(self):
        """Test validation with a valid sink config."""
        valid_config = {
            "connector.class": "MongoDbAtlasSink",
            "name": "test-sink-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret"
        }
        
        result = validate_unified_config(valid_config, "test.json")
        self.assertTrue(result)
    
    def test_missing_connector_class(self):
        """Test validation fails when connector.class is missing."""
        invalid_config = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret"
        }
        
        result = validate_unified_config(invalid_config, "test.json")
        self.assertFalse(result)
    
    def test_missing_name_field(self):
        """Test validation fails when name field is missing."""
        invalid_config = {
            "connector.class": "MongoDbAtlasSource",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret"
        }
        
        result = validate_unified_config(invalid_config, "test.json")
        self.assertFalse(result)
    
    def test_invalid_connector_class(self):
        """Test validation fails with invalid connector.class."""
        invalid_config = {
            "connector.class": "InvalidConnector",
            "name": "test-processor",
            "kafka.api.key": "test-key"
        }
        
        result = validate_unified_config(invalid_config, "test.json")
        self.assertFalse(result)
    
    def test_extra_fields_allowed(self):
        """Test that extra fields don't break validation."""
        config_with_extras = {
            "connector.class": "MongoDbAtlasSource",
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "topic.prefix": "test-prefix",
            "database": "testdb",
            "collection": "testcoll",
            "extra.field": "extra-value"
        }
        
        result = validate_unified_config(config_with_extras, "test.json")
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()