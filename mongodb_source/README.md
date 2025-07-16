# Kafka Topic Creator CLI

A command-line tool that automatically creates Kafka topics using the Confluent REST API based on connector configuration files.

## Overview

This tool reads a main configuration file containing Confluent cluster information and processes multiple connector configuration files to create corresponding Kafka topics. Each topic is created with the naming convention `<topic.prefix>.<database>.<collection>`.

## Features

- **Batch processing**: Process multiple connector configs in one run
- **Automatic topic naming**: Creates topics using the format `prefix.database.collection`
- **Error handling**: Comprehensive error handling with clear logging
- **Progress tracking**: Shows success/failure status for each topic creation
- **Duplicate handling**: Gracefully handles existing topics
- **JSON validation**: Validates configuration files before processing

## Requirements

- Python 3.6 or higher
- Internet connection (for API calls to Confluent)

## Installation

1. **Clone or download** the project files to your local machine

2. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

   Or install manually:
   ```bash
   pip install requests
   ```

## Configuration

### Main Configuration File

Create a JSON file with your Confluent cluster information:

```json
{
    "confluent-cluster-id": "your-cluster-id",
    "confluent-rest-endpoint": "https://your-rest-endpoint.com"
}
```

**Example** (`main_config.json`):
```json
{
    "confluent-cluster-id": "lkc-abc123",
    "confluent-rest-endpoint": "https://pkc-xyz789.us-west-2.aws.confluent.cloud"
}
```

### Connector Configuration Files

Create individual JSON files for each connector. Each file should contain:

```json
{
    "connector.class": "MongoDbAtlasSource",
    "name": "my-connector-name",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "your-kafka-api-key",
    "kafka.api.secret": "your-kafka-api-secret",
    "topic.prefix": "mongodb",
    "connection.host": "cluster0.abc123.mongodb.net",
    "connection.user": "dbuser",
    "connection.password": "dbpassword",
    "database": "inventory",
    "collection": "products",
    "poll.await.time.ms": "5000",
    "poll.max.batch.size": "1000",
    "startup.mode": "copy_existing",
    "output.data.format": "JSON",
    "tasks.max": "1"
}
```

**Required fields for topic creation**:
- `kafka.api.key`: API key for authentication
- `kafka.api.secret`: API secret for authentication
- `topic.prefix`: Prefix for the topic name
- `database`: Database name
- `collection`: Collection name

## Usage

### Basic Usage

```bash
python kafka_topic_creator.py <main_config.json> <connector_configs_folder>
```

### Examples

**Example 1: Basic usage**
```bash
python kafka_topic_creator.py main_config.json ./connector_configs/
```

**Example 2: With absolute paths**
```bash
python kafka_topic_creator.py /path/to/main_config.json /path/to/connector_configs/
```

**Example 3: Current directory**
```bash
python kafka_topic_creator.py config.json .
```

## Project Structure

```
kafka-topic-creator/
├── kafka_topic_creator.py    # Main application
├── requirements.txt          # Python dependencies
├── README.md                # This file
├── main_config.json         # Main configuration (create this)
└── connector_configs/       # Folder with connector configs (create this)
    ├── connector1.json
    ├── connector2.json
    └── connector3.json
```

## Topic Configuration

Topics are created with the following default settings:
- **Partitions**: 3
- **Cleanup Policy**: delete
- **Naming Convention**: `<topic.prefix>.<database>.<collection>`

For example, if your connector config has:
- `topic.prefix`: "mongodb"
- `database`: "inventory"
- `collection`: "products"

The created topic will be named: `mongodb.inventory.products`

## Output Examples

### Successful Run
```
Loading main configuration...
✓ Main config loaded successfully
  Cluster ID: lkc-abc123
  REST Endpoint: https://pkc-xyz789.us-west-2.aws.confluent.cloud

Found 3 .json files to process
--------------------------------------------------

Processing: connector1.json
✓ Successfully created topic: mongodb.inventory.products

Processing: connector2.json
✓ Successfully created topic: mongodb.inventory.orders

Processing: connector3.json
⚠ Topic already exists: mongodb.users.profiles
--------------------------------------------------
Summary: 3/3 topics processed successfully
```

### Error Handling
```
Processing: bad_connector.json
Error: Missing required field 'kafka.api.key' in bad_connector.json

Processing: network_issue.json
✗ Network error creating topic mongodb.test.collection: Connection timeout
```

## Troubleshooting

### Common Issues

1. **Missing dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Invalid JSON files**:
   - Check JSON syntax using a JSON validator
   - Ensure all required fields are present

3. **API authentication errors**:
   - Verify your API key and secret are correct
   - Check that the API key has proper permissions

4. **Network issues**:
   - Ensure you have internet connectivity
   - Check if the REST endpoint URL is correct
   - Verify firewall settings

5. **Topic already exists**:
   - This is normal behavior and will be logged as a warning
   - The script will continue processing other topics

### Debug Mode

For more verbose output, you can modify the script or add print statements to see detailed request/response information.

## API Reference

The tool uses the Confluent REST API v3 endpoint:
```
POST /kafka/v3/clusters/{cluster_id}/topics
```

For more information, see the [Confluent REST API documentation](https://docs.confluent.io/platform/current/kafka-rest/api.html).

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is provided as-is for educational and development purposes.