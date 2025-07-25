#!/usr/bin/env python3
"""
Unit tests for sink connector configuration validation functionality.
"""

import unittest

# Add parent directory to path to import from processors
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.sink import validate_connector_config


class TestSinkValidation(unittest.TestCase):
    """Test cases for sink connector config validation."""
    
    def test_valid_config(self):
        """Test validation with a complete, valid config."""
        valid_config = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": "test.topic",
            "database": "testdb",
            "collection": "testcoll"
        }
        
        is_valid, issues = validate_connector_config(valid_config, "test.json")
        self.assertTrue(is_valid)
        self.assertEqual(len(issues), 0)
    
    def test_missing_required_field(self):
        """Test validation fails when required field is missing."""
        invalid_config = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            # Missing input.data.format
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": "test.topic",
            "database": "testdb",
            "collection": "testcoll"
        }
        
        is_valid, issues = validate_connector_config(invalid_config, "test.json")
        self.assertFalse(is_valid)
        self.assertIn("Missing required field 'input.data.format'", issues)
    
    def test_missing_name_field(self):
        """Test validation fails when name field is missing."""
        invalid_config = {
            # Missing name field
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": "test.topic",
            "database": "testdb",
            "collection": "testcoll"
        }
        
        is_valid, issues = validate_connector_config(invalid_config, "test.json")
        self.assertFalse(is_valid)
        self.assertIn("Missing required field 'name'", issues)
    
    def test_topics_as_array(self):
        """Test that topics field can be an array."""
        config_with_array_topics = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": ["topic1", "topic2", "topic3"],
            "database": "testdb",
            "collection": "testcoll"
        }
        
        is_valid, issues = validate_connector_config(config_with_array_topics, "test.json")
        self.assertTrue(is_valid)
        self.assertEqual(len(issues), 0)
    
    def test_empty_topics_array(self):
        """Test that empty topics array is invalid."""
        invalid_config = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": [],  # Empty array
            "database": "testdb",
            "collection": "testcoll"
        }
        
        is_valid, issues = validate_connector_config(invalid_config, "test.json")
        self.assertFalse(is_valid)
        self.assertIn("'topics' array cannot be empty", issues)
    
    def test_invalid_topics_type(self):
        """Test that invalid topics type is rejected."""
        invalid_config = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": 123,  # Invalid type
            "database": "testdb",
            "collection": "testcoll"
        }
        
        is_valid, issues = validate_connector_config(invalid_config, "test.json")
        self.assertFalse(is_valid)
        self.assertIn("'topics' field must be a string or array", issues)
    
    def test_timeseries_parameters_rejected(self):
        """Test that timeseries parameters are rejected."""
        config_with_timeseries = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": "test.topic",
            "database": "testdb",
            "collection": "testcoll",
            "timeseries.timestamp.field": "timestamp",
            "timeseries.metadata.field": "metadata",
            "timeseries.granularity": "seconds"
        }
        
        is_valid, issues = validate_connector_config(config_with_timeseries, "test.json")
        self.assertFalse(is_valid)
        
        # Check that all timeseries parameters are flagged
        timeseries_issues = [issue for issue in issues if "timeseries" in issue]
        self.assertEqual(len(timeseries_issues), 3)
        
        # Check specific message
        self.assertTrue(any("this converter does not currently support timeseries" in issue for issue in issues))
    
    def test_delete_on_null_values_rejected(self):
        """Test that delete.on.null.values parameter is rejected."""
        config_with_delete_null = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": "test.topic",
            "database": "testdb",
            "collection": "testcoll",
            "delete.on.null.values": "true"
        }
        
        is_valid, issues = validate_connector_config(config_with_delete_null, "test.json")
        self.assertFalse(is_valid)
        self.assertTrue(any("delete.on.null.values" in issue and "not supported in stream processors" in issue for issue in issues))
    
    def test_multiple_issues_tracked(self):
        """Test that multiple validation issues are tracked."""
        config_with_multiple_issues = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            # Missing kafka.api.secret
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": [],  # Empty array
            "database": "testdb",
            "collection": "testcoll",
            "timeseries.timestamp.field": "timestamp",  # Unsupported
            "delete.on.null.values": "true"  # Unsupported
        }
        
        is_valid, issues = validate_connector_config(config_with_multiple_issues, "test.json")
        self.assertFalse(is_valid)
        
        # Should have at least 4 issues
        self.assertGreaterEqual(len(issues), 4)
        
        # Check for specific issues
        issue_text = " ".join(issues)
        self.assertIn("Missing required field 'kafka.api.secret'", issue_text)
        self.assertIn("'topics' array cannot be empty", issue_text)
        self.assertIn("timeseries", issue_text)
        self.assertIn("delete.on.null.values", issue_text)
    
    def test_extra_fields_allowed(self):
        """Test that extra (non-unsupported) fields don't break validation."""
        config_with_extras = {
            "name": "test-processor",
            "kafka.api.key": "test-key",
            "kafka.api.secret": "test-secret",
            "input.data.format": "JSON",
            "connection.user": "test-user",
            "connection.password": "test-pass",
            "topics": "test.topic",
            "database": "testdb",
            "collection": "testcoll",
            "consumer.override.auto.offset.reset": "earliest",  # Supported extra field
            "custom.field": "custom-value"  # Random extra field
        }
        
        is_valid, issues = validate_connector_config(config_with_extras, "test.json")
        self.assertTrue(is_valid)
        self.assertEqual(len(issues), 0)


if __name__ == '__main__':
    unittest.main()