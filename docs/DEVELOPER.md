# Developer Documentation

This document provides technical details for developers working on the MongoDB Atlas Stream Processing configuration tools.

## Configuration Validation System

### Overview

The project includes a comprehensive configuration validation system that ensures MongoDB connector configurations are valid before processing. This system automatically validates both general configuration properties and connector-specific properties.

### How It Works

The validation system uses a rule-based approach where validation rules are defined in CSV files:

1. **Auto-Detection**: The system automatically detects whether a configuration is for a source or sink connector by examining the `connector.class` field
2. **Multi-File Validation**: Loads and applies rules from multiple CSV files simultaneously
3. **Comprehensive Validation**: Validates all configuration properties against their specific rules

### Validation Rule Types

#### REQUIRE
- **Purpose**: Fields that must be present in the configuration
- **Behavior**: 
  - If field is missing → Validation fails with error "Missing required fields: {field_name}"
  - If field is present → Continues validation

#### IGNORE  
- **Purpose**: Fields that are optional and don't need validation
- **Behavior**: Field presence or absence doesn't affect validation (always passes)

#### DISALLOW
- **Purpose**: Fields that are not supported and must not be present
- **Behavior**:
  - If field is absent → Passes
  - If field is present → Validation fails with error "The following fields are not supported: {field_name}"

#### ALLOW default
- **Purpose**: Fields that only accept their default value
- **Behavior**:
  - If field is absent → Passes (treated like IGNORE)
  - If field equals default value → Passes
  - If field has different value → Validation fails with error "Only {default_value} is supported for {field_name}"

#### ALLOW {values}
- **Purpose**: Fields that only accept specific allowed values
- **Behavior**:
  - If field is absent → Passes (treated like IGNORE)
  - If field equals one of allowed values → Passes
  - If field has different value → Validation fails with error "Only {value1}, {value2} is supported for {field_name}"

### CSV Rule Files

The validation rules are stored in CSV files located in `processors/rules/`:

#### `general_managed_configs.csv`
- Contains rules for configuration properties common to both source and sink connectors
- Always loaded regardless of connector type
- Example fields: `name`, `kafka.auth.mode`, `connector.class`

#### `managed_source_configs.csv`
- Contains rules specific to source connectors
- Loaded when `connector.class` contains "Source"
- Example fields: `topic.prefix`, `startup.mode`, `output.data.format`

#### `managed_sink_configs.csv`  
- Contains rules specific to sink connectors
- Loaded when `connector.class` contains "Sink"
- Example fields: `topics`, `input.data.format`, `write.strategy`

### CSV File Format

Each CSV file has the following columns:

| Column | Description | Example |
|--------|-------------|---------|
| `#` | Row number | 1, 2, 3... |
| `subsection` | Grouping category | "Kafka Cluster credentials" |
| `name` | Configuration field name | "kafka.api.key" |
| `definition` | Human-readable description | "Kafka API Key for authentication" |
| `type` | Data type | "string", "boolean", "int" |
| `what_do_do` | Validation action | "REQUIRE", "ALLOW default", "ALLOW JSON" |
| `default` | Default value (for ALLOW default) | "KAFKA_API_KEY" |
| `valid_values` | Comma-separated valid values | "JSON, AVRO, STRING" |
| `importance` | Priority level | "high", "medium", "low" |

### API Usage

#### Basic Usage
```python
from processors.config_validator import validate_connector_config

# Auto-detect connector type from connector.class
config = {
    'connector.class': 'com.mongodb.kafka.connect.MongoSourceConnector',
    'name': 'my-connector',
    # ... other config fields
}

result = validate_connector_config(config)

if result.is_valid:
    print("Configuration is valid!")
else:
    print("Validation errors:")
    for error in result.error_messages:
        print(f"  - {error}")
```

#### Manual Type Override
```python
# Manually specify connector type
result = validate_connector_config(config, connector_type='source')
```

#### Validation Result Structure
```python
@dataclass
class ValidationResult:
    is_valid: bool                    # Overall validation status
    missing_required: List[str]       # List of missing required fields
    disallowed_present: List[str]     # List of disallowed fields that were present
    error_messages: List[str]         # Human-readable error messages
```

### Architecture Details

#### Class Structure

```python
class ValidationAction(Enum):
    REQUIRE = "REQUIRE"
    IGNORE = "IGNORE" 
    DISALLOW = "DISALLOW"
    ALLOW_DEFAULT = "ALLOW default"
    ALLOW_VALUES = "ALLOW_VALUES"
    ALLOW = "ALLOW"

class ValidationRule:
    name: str                         # Field name
    action: ValidationAction          # Validation type
    default_value: Optional[str]      # For ALLOW_DEFAULT rules
    allowed_values: Optional[List[str]] # For ALLOW_VALUES rules
    # ... other metadata

class ConfigValidator:
    def load_rules_from_csv(...)      # Load rules from CSV file
    def validate_config(...)          # Validate a configuration
```

#### Auto-Detection Logic

```python
def validate_connector_config(config, connector_type=None):
    if connector_type is None:
        connector_class = config.get('connector.class', '')
        if 'Source' in connector_class:
            connector_type = 'source'
        elif 'Sink' in connector_class:
            connector_type = 'sink'
        else:
            raise ValueError("Cannot determine connector type")
    
    # Load appropriate rule files based on connector type
    # ...
```

### Testing
The project includes comprehensive unit and integration tests to ensure code quality and reliability.

#### Test Structure
```
tests/
├── run_tests.py              # Main test runner
├── unit/
│   └── test_csv_validation.py # CSV validation rule tests
└── integration/
    ├── test_integration.py    # E2E tests with real processors
    ├── integration_configs/   # Test configuration files
    └── .env.integration       # Integration test environment vars
```

#### Test Types

- **Unit Tests**: Fast CSV validation rule tests that don't require external services
- **Integration Tests**: Full end-to-end tests that create real MongoDB Atlas Stream Processors

#### Run All Tests
```bash
# Run all tests (unit + integration)
python tests/run_tests.py

# Run with verbose output
python tests/run_tests.py -v
```

#### Run Specific Test Types
```bash
# Run only unit tests (fast, no external dependencies)
python tests/run_tests.py --unit-only

# Run only integration tests (requires setup)
python tests/run_tests.py --integration-only
```

#### Unit Tests
Located in `tests/unit/test_csv_validation.py`:
- **IGNORE rule validation**: Tests that ignored fields don't cause validation failures
- **DISALLOW rule validation**: Tests that disallowed fields cause specific validation errors
- **REQUIRE rule validation**: Tests that missing required fields cause validation failures
- **ALLOW rule validation**: Tests that ALLOW fields accept valid values and reject invalid ones
- **Source vs Sink differentiation**: Tests that source and sink configs use their respective CSV rules
- **Verbose mode**: Use `-v` flag to see detailed validation output for debugging

#### Integration Tests
Located in `tests/integration/test_integration.py`:

**Purpose**: Validate that CSV rules work in the full `create_processors.py` pipeline by creating real MongoDB Atlas Stream Processors.

**Setup Requirements**:
1. **Atlas CLI Authentication**: Tests automatically run `atlas auth login`
2. **Environment Variables**: Create `tests/integration/.env.integration` with test credentials:
   ```bash
   test_kafka_api_key="your-key"
   test_kafka_api_secret="your-secret"
   test_db_user="your-username"
   test_db_password="your-password"
   test_confluent_cluster_id="your-cluster-id"
   test_confluent_rest_endpoint="https://your-endpoint:443"
   test_stream_processor_url="mongodb://your-stream-url/"
   test_tenant_name="your-tenant"
   test_group_id="your-group-id"
   test_cluster_name="your-cluster"
   test_topic="test_topic"
   test_database="test_db"
   ```

**How Integration Tests Work**:
1. **Discovery**: Automatically finds all `*.json` files in `tests/integration/integration_configs/`
2. **Config Generation**: Creates complete connector configs by merging test configs with environment variables
3. **Pipeline Execution**: Runs `create_processors.py` with the generated configs
4. **Verification**: Uses `mongosh sp.<processor-name>.stats()` to verify processors were created in Atlas
5. **Cleanup**: Automatically deletes test processors after tests complete

**Adding New Integration Tests**:
Create minimal JSON files in `tests/integration/integration_configs/` with only the fields you want to test:

```json
{
    "connector.class": "MongoDbAtlasSink",
    "name": "test-processor-name",
    "consumer.override.auto.offset.reset": "earliest"
}
```

The test framework automatically adds required fields (credentials, topics, database, etc.) from environment variables.

**Integration Test Examples**:
- `sink_auto_offset_earliest.json`: Tests that `consumer.override.auto.offset.reset: "earliest"` works
- Future tests can validate other ALLOW field combinations, DISALLOW field rejection, etc.

**What Integration Tests Prove**:
- CSV validation rules work in the full pipeline
- Stream processors are actually created in MongoDB Atlas
- Processors can be started and are functional
- No bugs exist between validation and processor creation

#### Running Tests

```bash
# Run specific test files
python tests/unit/test_csv_validation.py -v
python tests/integration/test_integration.py

# Run CSV validation tests with detailed output
python tests/unit/test_csv_validation.py -v
```

**Note**: Integration tests require proper Atlas CLI authentication and valid test credentials. Unit tests have no external dependencies.

#### Running Tests
```bash
# Run all validation tests
cd tests && python run_tests.py --unit-only

# Run specific test file
python -m unittest tests.unit.test_config_validator -v
```

### Extending the System

#### Adding New Validation Types

1. **Add to ValidationAction enum**:
```python
class ValidationAction(Enum):
    # ... existing actions
    NEW_ACTION = "NEW_ACTION"
```

2. **Update validation logic** in `ConfigValidator.validate_config()`:
```python
elif rule.action == ValidationAction.NEW_ACTION:
    # Implement new validation logic
    if some_condition:
        error_messages.append("Custom error message")
```

3. **Add tests** for the new validation type

#### Adding New Rule Files

1. **Create new CSV file** in `processors/rules/`
2. **Update `validate_connector_config()`** to load the new file when appropriate
3. **Add tests** to verify the new rules are loaded and applied

#### Modifying CSV Rules

1. **Edit the appropriate CSV file** in `processors/rules/`
2. **Test changes** with both unit and integration tests
3. **Verify backward compatibility** with existing configurations

### Best Practices

#### Rule Design
- **Be specific**: Use precise field names and clear error messages
- **Group related fields**: Use consistent subsection names
- **Document thoroughly**: Provide clear definitions for each field

#### Code Maintenance  
- **Test thoroughly**: Add tests for any new validation logic
- **Keep CSV files in sync**: Ensure rule files match actual connector behavior
- **Monitor performance**: Validation should be fast for large configurations

#### Error Messages
- **Be helpful**: Explain what's wrong and what values are acceptable
- **Be consistent**: Use similar phrasing across error types
- **Be specific**: Include field names and expected values

### Common Issues

#### CSV Parsing Problems
- **Commas in values**: Ensure proper CSV escaping for values containing commas
- **Encoding issues**: Use UTF-8 encoding for all CSV files
- **Empty rows**: System skips empty rows automatically

#### Rule Conflicts
- **Multiple rules for same field**: Later rules override earlier ones
- **Contradictory rules**: Avoid having both REQUIRE and DISALLOW for same field

#### Performance
- **Large rule sets**: System efficiently loads and caches rules
- **Frequent validation**: Consider caching ValidationResult objects if needed

## Project Structure

### Core Components

```
processors/
├── __init__.py
├── common.py                 # Shared utility functions
├── config_validator.py       # Configuration validation system
├── sink.py                   # Sink processor creation
├── source.py                 # Source processor creation
└── rules/                    # Validation rule files
    ├── general_managed_configs.csv
    ├── managed_source_configs.csv
    └── managed_sink_configs.csv
```

### Test Structure

```
tests/
├── __init__.py
├── run_tests.py              # Unified test runner
├── unit/                     # Fast, mocked tests
│   ├── test_config_validator.py
│   ├── test_auth.py
│   ├── test_connections.py
│   └── ...
└── integration/              # End-to-end tests
    ├── test_auto_detection.py
    ├── test_real_validation.py
    └── ...
```

### Legacy Structure (Reference)

Some older files may still reference the previous structure:
- `source_configuration_properties.csv` → `managed_source_configs.csv`
- `sink_configuration_properties.csv` → `managed_sink_configs.csv`

## MongoDB Atlas Stream Processing Integration

### Connection Management

The tools create reusable connections that can be shared across multiple stream processors:

#### MongoDB Connections
- Created using Atlas CLI (`atlas streams connections create`)
- Configured with cluster name and database role
- Reused across multiple processors

#### Kafka Connections  
- Created using Atlas CLI with SASL_SSL security
- Configured with API key/secret authentication
- Supports auto.offset.reset configuration

### Stream Processor Creation

#### Source Processors (MongoDB → Kafka)
```javascript
// Generated pipeline structure
[
  {
    "$source": {
      "connectionName": "mongodb-connection",
      "db": "database-name", 
      "coll": "collection-name"
    }
  },
  {
    "$emit": {
      "connectionName": "kafka-connection",
      "topic": "topic-name"
    }
  }
]
```

#### Sink Processors (Kafka → MongoDB)
```javascript
// Generated pipeline structure
[
  {
    "$source": {
      "connectionName": "kafka-connection",
      "topic": "topic-name",
      "config": {
        "auto_offset_reset": "earliest"
      }
    }
  },
  {
    "$merge": {
      "into": {
        "connectionName": "mongodb-connection",
        "db": "database-name",
        "coll": "collection-name" 
      }
    }
  }
]
```

### Error Handling

The system includes comprehensive error handling:

1. **Pre-validation**: Configuration validation before processing
2. **Connection errors**: Graceful handling of connection creation failures  
3. **Stream processor errors**: Clear error messages for pipeline creation issues
4. **Rollback capability**: Can clean up partially created resources

### API Integration

#### Atlas CLI Commands
- `atlas auth whoami` - Check authentication
- `atlas streams connections create` - Create connections
- `atlas streams connection create` - Alternative connection creation

#### MongoDB Shell (mongosh)
- `sp.createStreamProcessor()` - Create stream processors
- Connects to Atlas Stream Processing instances
- Executes JavaScript commands for pipeline creation

#### Confluent REST API
- Topic creation for source processors
- Cluster validation
- Authentication via API key/secret

## Development Workflow

### Setting Up Development Environment

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Authenticate with Atlas**:
```bash
atlas auth login
```

3. **Run tests to verify setup**:
```bash
cd tests && python run_tests.py --unit-only
```

### Making Changes

1. **Follow test-driven development**:
   - Write tests for new functionality
   - Implement the functionality
   - Verify all tests pass

2. **Update validation rules**:
   - Modify CSV files as needed
   - Test with integration tests
   - Verify backward compatibility

3. **Document changes**:
   - Update this developer documentation
   - Update user-facing README if needed
   - Add inline code comments for complex logic

### Release Process

1. **Run full test suite**:
```bash
cd tests && python run_tests.py -v
```

2. **Verify integration tests** (requires Atlas auth):
```bash
cd tests && python run_tests.py --integration-only
```

3. **Test with real configurations**:
```bash
# Test validation system
python tests/integration/test_real_validation.py
python tests/integration/test_auto_detection.py
```

4. **Update version information** and release notes

This completes the developer documentation covering the configuration validation system, architecture details, and development workflows.