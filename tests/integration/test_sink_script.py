#!/usr/bin/env python3
"""
Integration tests for create_sink_processors.py script.

These tests require Atlas CLI authentication and test the full script execution.
"""

import unittest
import subprocess
import tempfile
import json
import os
import shutil

# Add parent directory to path to import from common
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.common import check_atlas_auth_with_login


class TestSinkProcessorScript(unittest.TestCase):
    """Integration tests for the sink processor script."""
    
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Check authentication before running any tests
        if not check_atlas_auth_with_login():
            self.skipTest("Atlas CLI authentication required for integration tests")
        
        # Create temporary test config
        self.test_config = {
            "confluent-cluster-id": "test-cluster-id",
            "confluent-rest-endpoint": "https://test-endpoint.com:443",
            "mongodb-stream-processor-instance-url": "mongodb://test-url/",
            "stream-processor-prefix": "integration-test-sink",
            "kafka-connection-name": "test-kafka-connection",
            "mongodb-connection-name": "test-mongodb-connection",
            "mongodb-cluster-name": "TestCluster",
            "mongodb-group-id": "507f1f77bcf86cd799439011",
            "mongodb-tenant-name": "test-tenant"
        }
        
        # Create temporary config file
        self.temp_config = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
        json.dump(self.test_config, self.temp_config, indent=2)
        self.temp_config.close()
        
        # Create temporary connector configs directory
        self.temp_dir = tempfile.mkdtemp()
        
        # Create test sink connector config
        connector_config = {
            "kafka.api.key": "test-api-key",
            "kafka.api.secret": "test-api-secret",
            "input.data.format": "JSON",
            "connection.user": "testuser", 
            "connection.password": "testpass",
            "topics": "test.topic.testdb.testcoll",
            "database": "testdb",
            "collection": "testcoll"
        }
        
        config_file = os.path.join(self.temp_dir, "test_sink_config.json")
        with open(config_file, 'w') as f:
            json.dump(connector_config, f, indent=2)
    
    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Clean up temporary files
        try:
            os.unlink(self.temp_config.name)
            shutil.rmtree(self.temp_dir)
        except:
            pass  # Best effort cleanup
    
    def test_script_loads_config_successfully(self):
        """Test that the script can load and validate the config."""
        cmd = ['python3', 'processors/sink.py', self.temp_config.name, self.temp_dir]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Script should load config successfully (even if it fails later due to auth/network)
            self.assertIn("Loading main configuration", result.stdout)
            self.assertIn("Main config loaded successfully", result.stdout)
            
        except subprocess.TimeoutExpired:
            self.fail("Script timed out - may indicate hanging")
    
    def test_script_validates_config_fields(self):
        """Test that the script validates all required config fields."""
        cmd = ['python3', 'processors/sink.py', self.temp_config.name, self.temp_dir]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Should not fail validation (may fail later for other reasons)
            self.assertNotIn("Error: Missing required field", result.stdout)
            self.assertNotIn("Error: Missing required field", result.stderr)
            
        except subprocess.TimeoutExpired:
            self.fail("Script timed out during config validation")
    
    def test_script_handles_missing_connector_configs(self):
        """Test that the script handles missing connector config files gracefully."""
        # Create empty directory
        empty_dir = tempfile.mkdtemp()
        
        try:
            cmd = ['python3', 'processors/sink.py', self.temp_config.name, empty_dir]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Should handle missing configs gracefully
            self.assertTrue(
                "No .json files found" in result.stdout or
                result.returncode == 0  # Script completes successfully
            )
            
        finally:
            shutil.rmtree(empty_dir)
    
    def test_script_displays_config_summary(self):
        """Test that the script displays the loaded configuration summary."""
        cmd = ['python3', 'processors/sink.py', self.temp_config.name, self.temp_dir]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Should display config summary
            self.assertIn("Confluent Cluster ID:", result.stdout)
            self.assertIn("Stream Processor URL:", result.stdout)
            self.assertIn("Kafka Connection Name:", result.stdout)
            
        except subprocess.TimeoutExpired:
            self.fail("Script timed out before displaying config summary")
    
    def test_script_handles_topic_array_format(self):
        """Test that the script handles topics as both string and array."""
        # Create config with topics as array
        connector_config = {
            "kafka.api.key": "test-api-key",
            "kafka.api.secret": "test-api-secret",
            "input.data.format": "JSON",
            "connection.user": "testuser", 
            "connection.password": "testpass",
            "topics": ["topic1", "topic2", "topic3"],  # Array format
            "database": "testdb",
            "collection": "testcoll"
        }
        
        config_file = os.path.join(self.temp_dir, "test_array_topics.json")
        with open(config_file, 'w') as f:
            json.dump(connector_config, f, indent=2)
        
        cmd = ['python3', 'processors/sink.py', self.temp_config.name, self.temp_dir]
        
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            # Should handle array topics without validation errors
            self.assertNotIn("Error: 'topics' field must be", result.stdout)
            self.assertNotIn("Error: 'topics' array cannot be empty", result.stdout)
            
        except subprocess.TimeoutExpired:
            self.fail("Script timed out handling array topics")


if __name__ == '__main__':
    unittest.main()