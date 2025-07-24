#!/usr/bin/env python3
"""
Unit tests for JSON file loading functionality.
"""

import unittest
import tempfile
import os
import json

# Add parent directory to path to import from common
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.common import load_json_file


class TestLoadJsonFile(unittest.TestCase):
    """Test cases for load_json_file function."""
    
    def test_load_valid_json(self):
        """Test loading a valid JSON file."""
        test_data = {"test": "data", "number": 123}
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(test_data, f)
            temp_file = f.name
        
        try:
            result = load_json_file(temp_file)
            self.assertEqual(result, test_data)
        finally:
            os.unlink(temp_file)
    
    def test_load_nonexistent_file(self):
        """Test loading a file that doesn't exist."""
        result = load_json_file("/nonexistent/file.json")
        self.assertIsNone(result)
    
    def test_load_invalid_json(self):
        """Test loading a file with invalid JSON."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"invalid": json}')  # Missing quotes around 'json'
            temp_file = f.name
        
        try:
            result = load_json_file(temp_file)
            self.assertIsNone(result)
        finally:
            os.unlink(temp_file)
    
    def test_load_empty_file(self):
        """Test loading an empty file."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_file = f.name
        
        try:
            result = load_json_file(temp_file)
            self.assertIsNone(result)
        finally:
            os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main()