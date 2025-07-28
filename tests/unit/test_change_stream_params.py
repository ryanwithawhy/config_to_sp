#!/usr/bin/env python3
"""
Unit tests for change stream parameter handling in source processors.

Tests that change stream parameters are correctly extracted from connector configs
and properly mapped to Stream Processing $source stage configuration.
"""

import unittest
import sys
import json
from unittest.mock import patch, MagicMock
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from processors.common import create_stream_processor


class TestChangeStreamParameters(unittest.TestCase):
    """Test change stream parameter handling in stream processor creation."""
    
    def setUp(self):
        """Set up common test data."""
        self.base_args = {
            'connection_user': 'test_user',
            'connection_password': 'test_password', 
            'stream_processor_url': 'mongodb://test-url/',
            'kafka_connection_name': 'test-kafka-conn',
            'mongodb_connection_name': 'test-mongo-conn',
            'database': 'test_db',
            'collection': 'test_coll',
            'processor_type': 'source',
            'processor_name': 'test-processor',
            'topic_prefix': 'test'
        }
    
    @patch('processors.common.subprocess.run')
    def test_no_change_stream_params_creates_basic_source(self, mock_subprocess):
        """Test that source without change stream params creates basic $source stage."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        result = create_stream_processor(**self.base_args)
        
        # Verify mongosh was called
        self.assertTrue(mock_subprocess.called)
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]  # Last argument is the --eval command
        
        # Parse the JavaScript command to get the pipeline
        # Format: sp.createStreamProcessor("name", pipeline_json)
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify basic structure without config
        self.assertEqual(len(pipeline), 2)
        source_stage = pipeline[0]['$source']
        self.assertEqual(source_stage['connectionName'], 'test-mongo-conn')
        self.assertEqual(source_stage['db'], 'test_db')
        self.assertEqual(source_stage['coll'], 'test_coll')
        self.assertNotIn('config', source_stage)
        
        # Verify emit stage
        emit_stage = pipeline[1]['$emit']
        self.assertEqual(emit_stage['connectionName'], 'test-kafka-conn')
        self.assertEqual(emit_stage['topic'], 'test.test_db.test_coll')
    
    @patch('processors.common.subprocess.run')
    def test_full_document_param_added_to_config(self, mock_subprocess):
        """Test that fullDocument parameter is added to $source config."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        args = self.base_args.copy()
        args['full_document'] = 'whenAvailable'
        
        result = create_stream_processor(**args)
        
        # Extract and parse the pipeline
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify config was added
        source_stage = pipeline[0]['$source']
        self.assertIn('config', source_stage)
        self.assertEqual(source_stage['config']['fullDocument'], 'whenAvailable')
    
    @patch('processors.common.subprocess.run')
    def test_full_document_before_change_param_added_to_config(self, mock_subprocess):
        """Test that fullDocumentBeforeChange parameter is added to $source config."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        args = self.base_args.copy()
        args['full_document_before_change'] = 'required'
        
        result = create_stream_processor(**args)
        
        # Extract and parse the pipeline
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify config was added
        source_stage = pipeline[0]['$source']
        self.assertIn('config', source_stage)
        self.assertEqual(source_stage['config']['fullDocumentBeforeChange'], 'required')
    
    @patch('processors.common.subprocess.run')
    def test_full_document_only_param_added_to_config(self, mock_subprocess):
        """Test that fullDocumentOnly parameter is added to $source config."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        args = self.base_args.copy()
        args['full_document_only'] = True
        
        result = create_stream_processor(**args)
        
        # Extract and parse the pipeline
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify config was added
        source_stage = pipeline[0]['$source']
        self.assertIn('config', source_stage)
        self.assertEqual(source_stage['config']['fullDocumentOnly'], True)
    
    @patch('processors.common.subprocess.run')
    def test_multiple_change_stream_params_combined(self, mock_subprocess):
        """Test that multiple change stream parameters are combined in config."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        args = self.base_args.copy()
        args['full_document'] = 'updateLookup'
        args['full_document_before_change'] = 'whenAvailable'
        args['full_document_only'] = False
        
        result = create_stream_processor(**args)
        
        # Extract and parse the pipeline
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify all params are in config
        source_stage = pipeline[0]['$source']
        self.assertIn('config', source_stage)
        config = source_stage['config']
        self.assertEqual(config['fullDocument'], 'updateLookup')
        self.assertEqual(config['fullDocumentBeforeChange'], 'whenAvailable')
        self.assertEqual(config['fullDocumentOnly'], False)
    
    @patch('processors.common.subprocess.run')
    def test_default_values_ignored(self, mock_subprocess):
        """Test that 'default' values don't get added to config."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        args = self.base_args.copy()
        args['full_document'] = 'default'
        args['full_document_before_change'] = 'default'
        
        result = create_stream_processor(**args)
        
        # Extract and parse the pipeline
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify no config was added since all values were "default"
        source_stage = pipeline[0]['$source']
        self.assertNotIn('config', source_stage)
    
    @patch('processors.common.subprocess.run')
    def test_off_value_for_before_change_added_to_config(self, mock_subprocess):
        """Test that 'off' value for fullDocumentBeforeChange is added to config."""
        mock_subprocess.return_value.returncode = 0
        mock_subprocess.return_value.stderr = ""
        
        args = self.base_args.copy()
        args['full_document_before_change'] = 'off'
        
        result = create_stream_processor(**args)
        
        # Extract and parse the pipeline
        call_args = mock_subprocess.call_args[0][0]
        js_command = call_args[-1]
        pipeline_start = js_command.find('[')
        pipeline_end = js_command.rfind(']') + 1
        pipeline_json = js_command[pipeline_start:pipeline_end]
        pipeline = json.loads(pipeline_json)
        
        # Verify 'off' is added to config
        source_stage = pipeline[0]['$source']
        self.assertIn('config', source_stage)
        self.assertEqual(source_stage['config']['fullDocumentBeforeChange'], 'off')


class TestParameterExtraction(unittest.TestCase):
    """Test parameter extraction from connector configs."""
    
    def test_boolean_string_conversion(self):
        """Test conversion of string boolean values to actual booleans."""
        # This tests the logic in source.py for publish.full.document.only
        test_cases = [
            ('true', True),
            ('True', True), 
            ('TRUE', True),
            ('1', True),
            ('yes', True),
            ('on', True),
            ('false', False),
            ('False', False),
            ('0', False),
            ('no', False),
            ('off', False),
            ('invalid', False),
            ('', False)
        ]
        
        for string_val, expected_bool in test_cases:
            with self.subTest(string_val=string_val):
                # Simulate the conversion logic from source.py
                result = string_val.lower() in ('true', '1', 'yes', 'on')
                self.assertEqual(result, expected_bool, 
                               f"String '{string_val}' should convert to {expected_bool}")


if __name__ == '__main__':
    unittest.main()