#!/usr/bin/env python3
"""
Unit tests for the configuration validation system.
"""

import unittest
import tempfile
import os
import csv
from unittest.mock import patch

# Add parent directory to path to import from processors
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from processors.config_validator import (
    ConfigValidator, 
    ValidationRule, 
    ValidationAction, 
    ValidationResult,
    validate_connector_config
)


class TestConfigValidator(unittest.TestCase):
    """Test cases for ConfigValidator class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.validator = ConfigValidator()
    
    def create_test_csv(self, rules_data: list) -> str:
        """
        Create a temporary CSV file with test rules.
        
        Args:
            rules_data: List of dictionaries representing CSV rows
            
        Returns:
            Path to the temporary CSV file
        """
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        
        if rules_data:
            fieldnames = rules_data[0].keys()
            writer = csv.DictWriter(temp_file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rules_data)
        
        temp_file.close()
        return temp_file.name
    
    def tearDown(self):
        """Clean up any temporary files."""
        # Clean up is handled by the test methods that create temp files
        pass


class TestValidationRuleLoading(TestConfigValidator):
    """Test loading rules from CSV files."""
    
    def test_load_basic_rules(self):
        """Test loading basic validation rules from CSV."""
        rules_data = [
            {
                'name': 'required_field',
                'what_do_do': 'REQUIRE',
                'default': '',
                'valid_values': '',
                'subsection': 'Test Section',
                'definition': 'A required test field',
                'importance': 'high'
            },
            {
                'name': 'ignored_field', 
                'what_do_do': 'IGNORE',
                'default': 'default_val',
                'valid_values': 'val1,val2',
                'subsection': 'Test Section',
                'definition': 'An ignored test field',
                'importance': 'low'
            }
        ]
        
        csv_path = self.create_test_csv(rules_data)
        try:
            success = self.validator.load_rules_from_csv(csv_path, 'test')
            self.assertTrue(success)
            
            # Check that rules were loaded
            self.assertEqual(len(self.validator.rules), 2)
            self.assertIn('required_field', self.validator.rules)
            self.assertIn('ignored_field', self.validator.rules)
            
            # Check rule details
            required_rule = self.validator.rules['required_field']
            self.assertEqual(required_rule.action, ValidationAction.REQUIRE)
            self.assertEqual(required_rule.subsection, 'Test Section')
            
        finally:
            os.unlink(csv_path)
    
    def test_load_allow_default_rules(self):
        """Test loading ALLOW default rules."""
        rules_data = [
            {
                'name': 'default_field',
                'what_do_do': 'ALLOW default',
                'default': 'expected_default',
                'valid_values': '',
                'subsection': 'Test',
                'definition': 'Field with default value',
                'importance': 'medium'
            }
        ]
        
        csv_path = self.create_test_csv(rules_data)
        try:
            success = self.validator.load_rules_from_csv(csv_path)
            self.assertTrue(success)
            
            rule = self.validator.rules['default_field']
            self.assertEqual(rule.action, ValidationAction.ALLOW_DEFAULT)
            self.assertEqual(rule.default_value, 'expected_default')
            
        finally:
            os.unlink(csv_path)
    
    def test_load_nonexistent_file(self):
        """Test loading from a nonexistent file."""
        success = self.validator.load_rules_from_csv('/path/that/does/not/exist.csv')
        self.assertFalse(success)
    
    def test_load_multiple_files(self):
        """Test loading rules from multiple CSV files."""
        rules1 = [
            {'name': 'field1', 'what_do_do': 'REQUIRE', 'default': '', 'valid_values': '', 'subsection': '', 'definition': '', 'importance': ''}
        ]
        rules2 = [
            {'name': 'field2', 'what_do_do': 'DISALLOW', 'default': '', 'valid_values': '', 'subsection': '', 'definition': '', 'importance': ''}
        ]
        
        csv1_path = self.create_test_csv(rules1)
        csv2_path = self.create_test_csv(rules2)
        
        try:
            rule_files = [(csv1_path, 'file1'), (csv2_path, 'file2')]
            success = self.validator.load_multiple_rule_files(rule_files)
            self.assertTrue(success)
            
            self.assertEqual(len(self.validator.rules), 2)
            self.assertIn('field1', self.validator.rules)
            self.assertIn('field2', self.validator.rules)
            
        finally:
            os.unlink(csv1_path)
            os.unlink(csv2_path)


class TestRequireValidation(TestConfigValidator):
    """Test REQUIRE validation functionality."""
    
    def test_required_field_present(self):
        """Test validation passes when required field is present."""
        # Set up rules
        self.validator.rules['name'] = ValidationRule(
            name='name',
            action=ValidationAction.REQUIRE
        )
        
        config = {'name': 'test-connector'}
        result = self.validator.validate_config(config)
        
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.missing_required), 0)
        self.assertEqual(len(result.error_messages), 0)
    
    def test_required_field_missing(self):
        """Test validation fails when required field is missing."""
        # Set up rules
        self.validator.rules['name'] = ValidationRule(
            name='name',
            action=ValidationAction.REQUIRE
        )
        
        config = {}  # Missing required field
        result = self.validator.validate_config(config)
        
        self.assertFalse(result.is_valid)
        self.assertEqual(result.missing_required, ['name'])
        self.assertIn('Missing required fields: name', result.error_messages)
    
    def test_multiple_required_fields(self):
        """Test validation with multiple required fields."""
        # Set up rules
        self.validator.rules['name'] = ValidationRule(
            name='name',
            action=ValidationAction.REQUIRE
        )
        self.validator.rules['database'] = ValidationRule(
            name='database',
            action=ValidationAction.REQUIRE
        )
        self.validator.rules['collection'] = ValidationRule(
            name='collection',
            action=ValidationAction.REQUIRE
        )
        
        # Test all present
        config = {
            'name': 'test-connector',
            'database': 'test-db',
            'collection': 'test-coll'
        }
        result = self.validator.validate_config(config)
        self.assertTrue(result.is_valid)
        
        # Test some missing
        config = {'name': 'test-connector'}  # Missing database and collection
        result = self.validator.validate_config(config)
        
        self.assertFalse(result.is_valid)
        self.assertEqual(set(result.missing_required), {'database', 'collection'})
        self.assertIn('Missing required fields:', result.error_messages[0])
    
    def test_get_required_fields(self):
        """Test getting list of required fields."""
        self.validator.rules['field1'] = ValidationRule(name='field1', action=ValidationAction.REQUIRE)
        self.validator.rules['field2'] = ValidationRule(name='field2', action=ValidationAction.IGNORE)
        self.validator.rules['field3'] = ValidationRule(name='field3', action=ValidationAction.REQUIRE)
        
        required_fields = self.validator.get_required_fields()
        self.assertEqual(set(required_fields), {'field1', 'field3'})


class TestDisallowValidation(TestConfigValidator):
    """Test DISALLOW validation functionality."""
    
    def test_disallowed_field_absent(self):
        """Test validation passes when disallowed field is absent."""
        self.validator.rules['unsupported_field'] = ValidationRule(
            name='unsupported_field',
            action=ValidationAction.DISALLOW
        )
        
        config = {'name': 'test-connector'}  # No disallowed field
        result = self.validator.validate_config(config)
        
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.disallowed_present), 0)
    
    def test_disallowed_field_present(self):
        """Test validation fails when disallowed field is present."""
        self.validator.rules['unsupported_field'] = ValidationRule(
            name='unsupported_field',
            action=ValidationAction.DISALLOW
        )
        
        config = {'unsupported_field': 'some_value'}
        result = self.validator.validate_config(config)
        
        self.assertFalse(result.is_valid)
        self.assertEqual(result.disallowed_present, ['unsupported_field'])
        self.assertIn('The following fields are not supported: unsupported_field', result.error_messages)
    
    def test_multiple_disallowed_fields(self):
        """Test validation with multiple disallowed fields."""
        self.validator.rules['field1'] = ValidationRule(name='field1', action=ValidationAction.DISALLOW)
        self.validator.rules['field2'] = ValidationRule(name='field2', action=ValidationAction.DISALLOW)
        
        config = {'field1': 'value1', 'field2': 'value2', 'allowed_field': 'ok'}
        result = self.validator.validate_config(config)
        
        self.assertFalse(result.is_valid)
        self.assertEqual(set(result.disallowed_present), {'field1', 'field2'})


class TestAllowDefaultValidation(TestConfigValidator):
    """Test ALLOW default validation functionality."""
    
    def test_allow_default_correct_value(self):
        """Test validation passes when field has the correct default value."""
        self.validator.rules['mode'] = ValidationRule(
            name='mode',
            action=ValidationAction.ALLOW_DEFAULT,
            default_value='latest'
        )
        
        config = {'mode': 'latest'}
        result = self.validator.validate_config(config)
        
        self.assertTrue(result.is_valid)
        self.assertEqual(len(result.error_messages), 0)
    
    def test_allow_default_incorrect_value(self):
        """Test validation fails when field has incorrect value."""
        self.validator.rules['mode'] = ValidationRule(
            name='mode',
            action=ValidationAction.ALLOW_DEFAULT,
            default_value='latest'
        )
        
        config = {'mode': 'earliest'}
        result = self.validator.validate_config(config)
        
        self.assertFalse(result.is_valid)
        self.assertIn('Only latest is supported for mode', result.error_messages)
    
    def test_allow_default_field_absent(self):
        """Test validation passes when ALLOW default field is absent."""
        self.validator.rules['mode'] = ValidationRule(
            name='mode',
            action=ValidationAction.ALLOW_DEFAULT,
            default_value='latest'
        )
        
        config = {}  # Field not present
        result = self.validator.validate_config(config)
        
        self.assertTrue(result.is_valid)


class TestIgnoreValidation(TestConfigValidator):
    """Test IGNORE validation functionality."""
    
    def test_ignore_field_present(self):
        """Test IGNORE fields don't affect validation when present."""
        self.validator.rules['ignored_field'] = ValidationRule(
            name='ignored_field',
            action=ValidationAction.IGNORE
        )
        
        config = {'ignored_field': 'any_value'}
        result = self.validator.validate_config(config)
        
        self.assertTrue(result.is_valid)
    
    def test_ignore_field_absent(self):
        """Test IGNORE fields don't affect validation when absent."""
        self.validator.rules['ignored_field'] = ValidationRule(
            name='ignored_field',
            action=ValidationAction.IGNORE
        )
        
        config = {}
        result = self.validator.validate_config(config)
        
        self.assertTrue(result.is_valid)


class TestIntegration(TestConfigValidator):
    """Integration tests combining multiple validation types."""
    
    def test_mixed_validation_rules(self):
        """Test configuration with mixed validation rules."""
        # Set up mixed rules
        self.validator.rules.update({
            'name': ValidationRule(name='name', action=ValidationAction.REQUIRE),
            'database': ValidationRule(name='database', action=ValidationAction.REQUIRE),
            'mode': ValidationRule(name='mode', action=ValidationAction.ALLOW_DEFAULT, default_value='latest'),
            'unsupported': ValidationRule(name='unsupported', action=ValidationAction.DISALLOW),
            'optional': ValidationRule(name='optional', action=ValidationAction.IGNORE)
        })
        
        # Test valid config
        valid_config = {
            'name': 'test-connector',
            'database': 'test-db', 
            'mode': 'latest',
            'optional': 'some_value'
        }
        result = self.validator.validate_config(valid_config)
        self.assertTrue(result.is_valid)
        
        # Test invalid config with multiple issues
        invalid_config = {
            'name': 'test-connector',
            # Missing required 'database'
            'mode': 'earliest',  # Wrong default value
            'unsupported': 'bad_value',  # Disallowed field present
            'optional': 'ok'
        }
        result = self.validator.validate_config(invalid_config)
        
        self.assertFalse(result.is_valid)
        self.assertIn('database', result.missing_required)
        self.assertIn('unsupported', result.disallowed_present)
        self.assertIn('Only latest is supported for mode', result.error_messages)
        self.assertEqual(len(result.error_messages), 3)  # One for each type of error


class TestConvenienceFunction(unittest.TestCase):
    """Test the convenience function for connector config validation."""
    
    @patch('processors.config_validator.ConfigValidator.load_multiple_rule_files')
    @patch('os.path.exists')
    def test_validate_connector_config_source(self, mock_exists, mock_load):
        """Test validate_connector_config for source connector."""
        mock_exists.return_value = True
        mock_load.return_value = True
        
        # Mock a validator with some rules
        with patch('processors.config_validator.ConfigValidator.validate_config') as mock_validate:
            mock_validate.return_value = ValidationResult(
                is_valid=True,
                missing_required=[],
                disallowed_present=[],
                error_messages=[]
            )
            
            config = {'name': 'test-source'}
            result = validate_connector_config(config, 'source', '/test/path')
            
            self.assertTrue(result.is_valid)
            mock_load.assert_called_once()
    
    def test_validate_connector_config_invalid_type(self):
        """Test validate_connector_config with invalid connector type."""
        with self.assertRaises(ValueError):
            validate_connector_config({}, 'invalid_type')


if __name__ == '__main__':
    unittest.main()