#!/usr/bin/env python3
"""
Unit tests for Atlas CLI authentication functionality.
"""

import unittest
from unittest.mock import patch, MagicMock

# Add parent directory to path to import from common
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from common import check_atlas_auth_with_login


class TestCheckAtlasAuthWithLogin(unittest.TestCase):
    """Test cases for check_atlas_auth_with_login function."""
    
    @patch('subprocess.run')
    def test_already_authenticated(self, mock_run):
        """Test when user is already authenticated."""
        # Mock successful auth check
        mock_run.return_value = MagicMock(returncode=0)
        
        result = check_atlas_auth_with_login()
        
        self.assertTrue(result)
        mock_run.assert_called_once_with(
            ['atlas', 'auth', 'whoami'], 
            capture_output=True, 
            text=True, 
            timeout=10
        )
    
    @patch('builtins.input', return_value='y')
    @patch('subprocess.run')
    def test_not_authenticated_user_says_yes_login_succeeds(self, mock_run, mock_input):
        """Test when user is not authenticated, says yes to login, and login succeeds."""
        # First call (auth check) fails, second call (login) succeeds
        mock_run.side_effect = [
            MagicMock(returncode=1),  # Auth check fails
            MagicMock(returncode=0)   # Login succeeds
        ]
        
        result = check_atlas_auth_with_login()
        
        self.assertTrue(result)
        self.assertEqual(mock_run.call_count, 2)
        mock_input.assert_called_once()
    
    @patch('builtins.input', return_value='n')
    @patch('subprocess.run')
    def test_not_authenticated_user_says_no(self, mock_run, mock_input):
        """Test when user is not authenticated and says no to login."""
        # Auth check fails
        mock_run.return_value = MagicMock(returncode=1)
        
        result = check_atlas_auth_with_login()
        
        self.assertFalse(result)
        mock_run.assert_called_once()
        mock_input.assert_called_once()
    
    @patch('builtins.input', return_value='')  # Empty input (should default to yes)
    @patch('subprocess.run')
    def test_not_authenticated_empty_input_login_succeeds(self, mock_run, mock_input):
        """Test when user gives empty input (defaults to yes) and login succeeds."""
        mock_run.side_effect = [
            MagicMock(returncode=1),  # Auth check fails
            MagicMock(returncode=0)   # Login succeeds
        ]
        
        result = check_atlas_auth_with_login()
        
        self.assertTrue(result)
        self.assertEqual(mock_run.call_count, 2)
    
    @patch('builtins.input', return_value='y')
    @patch('subprocess.run')
    def test_not_authenticated_login_fails(self, mock_run, mock_input):
        """Test when user says yes to login but login fails."""
        mock_run.side_effect = [
            MagicMock(returncode=1),  # Auth check fails
            MagicMock(returncode=1)   # Login fails
        ]
        
        result = check_atlas_auth_with_login()
        
        self.assertFalse(result)
        self.assertEqual(mock_run.call_count, 2)
    
    @patch('builtins.input', side_effect=KeyboardInterrupt)
    @patch('subprocess.run')
    def test_keyboard_interrupt(self, mock_run, mock_input):
        """Test when user interrupts with Ctrl+C."""
        mock_run.return_value = MagicMock(returncode=1)
        
        result = check_atlas_auth_with_login()
        
        self.assertFalse(result)
    
    @patch('subprocess.run', side_effect=Exception("Network error"))
    def test_auth_check_exception(self, mock_run):
        """Test when auth check raises an exception."""
        result = check_atlas_auth_with_login()
        
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()