#!/usr/bin/env python3
"""
Unit tests for connection creation functionality.
"""

import unittest
from unittest.mock import patch, mock_open, MagicMock
import subprocess

# Add parent directory to path to import from common
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.common import create_kafka_connection, create_mongodb_connection


class TestCreateKafkaConnection(unittest.TestCase):
    """Test cases for create_kafka_connection function."""
    
    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    @patch('subprocess.run')
    def test_create_connection_success(self, mock_run, mock_remove, mock_exists):
        """Test successful Kafka connection creation."""
        mock_run.return_value = MagicMock(returncode=0)
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = create_kafka_connection(
                "test-group-id",
                "test-tenant",
                "test-connection",
                "https://test-endpoint.com:443",
                "test-api-key",
                "test-api-secret"
            )
        
        self.assertEqual(result, (True, True))
        mock_run.assert_called_once()
        mock_file.assert_called_once()
    
    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    @patch('subprocess.run')
    def test_create_connection_already_exists(self, mock_run, mock_remove, mock_exists):
        """Test when connection already exists."""
        mock_run.return_value = MagicMock(
            returncode=1, 
            stderr="Connection already exists"
        )
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = create_kafka_connection(
                "test-group-id",
                "test-tenant",
                "test-connection",
                "https://test-endpoint.com:443",
                "test-api-key",
                "test-api-secret"
            )
        
        self.assertEqual(result, (True, False))
    
    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    @patch('subprocess.run')
    def test_create_connection_failure(self, mock_run, mock_remove, mock_exists):
        """Test connection creation failure."""
        mock_run.return_value = MagicMock(
            returncode=1, 
            stderr="Some other error"
        )
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = create_kafka_connection(
                "test-group-id",
                "test-tenant",
                "test-connection",
                "https://test-endpoint.com:443",
                "test-api-key",
                "test-api-secret"
            )
        
        self.assertEqual(result, (False, False))
    
    @patch('subprocess.run', side_effect=subprocess.TimeoutExpired(['atlas'], 30))
    def test_create_connection_timeout(self, mock_run):
        """Test connection creation timeout."""
        with patch('builtins.open', mock_open()):
            result = create_kafka_connection(
                "test-group-id",
                "test-tenant",
                "test-connection",
                "https://test-endpoint.com:443",
                "test-api-key",
                "test-api-secret"
            )
        
        self.assertEqual(result, (False, False))


class TestCreateMongoDBConnection(unittest.TestCase):
    """Test cases for create_mongodb_connection function."""
    
    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    @patch('subprocess.run')
    def test_create_mongodb_connection_success(self, mock_run, mock_remove, mock_exists):
        """Test successful MongoDB connection creation."""
        mock_run.return_value = MagicMock(returncode=0)
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = create_mongodb_connection(
                "test-group-id",
                "test-tenant",
                "test-cluster",
                "test-connection",
                "readAnyDatabase"
            )
        
        self.assertEqual(result, (True, True))
        mock_run.assert_called_once()
        mock_file.assert_called_once()
    
    @patch('os.path.exists', return_value=True)
    @patch('os.remove')
    @patch('subprocess.run')
    def test_create_mongodb_connection_already_exists(self, mock_run, mock_remove, mock_exists):
        """Test when MongoDB connection already exists."""
        mock_run.return_value = MagicMock(
            returncode=1, 
            stderr="Connection already exists"
        )
        
        with patch('builtins.open', mock_open()) as mock_file:
            result = create_mongodb_connection(
                "test-group-id",
                "test-tenant",
                "test-cluster",
                "test-connection",
                "readWriteAnyDatabase"
            )
        
        self.assertEqual(result, (True, False))


if __name__ == '__main__':
    unittest.main()