# Kafka to MongoDB Stream Processor Setup Tool

This tool automatically sets up end-to-end streaming from Kafka topics to MongoDB collections using the MongoDB-Kafka configuration files you're already familiar with. All you need to do is create the stream processing instance that will run your processors and, once your processors are created, press start.

## Overview

This tool takes a [main configuration file](#main-configuration-file) with some information about your MongoDB Atlas cluster and stream processing instance and a [configuration file per collection](#connector-configuration-files) in the Confluent MongoDB Atlas Sink Connector format. It then processes the files to create:
1. **MongoDB Atlas Stream Processing Kafka connections** to read from Confluent Cloud
2. **MongoDB Atlas Stream Processing sink connections** to write to MongoDB clusters
3. **Stream processors** that automatically stream data from Kafka topics to MongoDB collections

## Features

- **Complete pipeline setup**: Creates the entire Kafka → MongoDB streaming pipeline
- **Bulk stream processor setup**: Create stream processors for multiple collections in a single command
- **Automatic naming**: Uses consistent naming conventions across all components
- **Error handling**: Comprehensive error handling with clear logging
- **Progress tracking**: Shows success/failure status for each creation step
- **Duplicate handling**: Gracefully handles existing connections and processors
- **JSON validation**: Validates configuration files before processing
- **Flexible topic support**: Supports both single topics and multiple topics per collection
- **Optional offset configuration**: Supports configurable auto offset reset behavior

## Requirements

- Python 3.6 or higher
- MongoDB Shell (mongosh) installed and in PATH
- MongoDB Atlas CLI installed and authenticated (`atlas auth login`)
- Internet connection (for API calls to Confluent Cloud and MongoDB Atlas)

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

Create a JSON file with your Confluent cluster and MongoDB Atlas information:

```json
{
    "confluent-cluster-id": "your-cluster-id",
    "confluent-rest-endpoint": "https://your-rest-endpoint.com",
    "mongodb-stream-processor-instance-url": "mongodb://your-stream-processor-instance-url/",
    "stream-processor-prefix": "managed-sink-test",
    "kafka-connection-name": "kafka-connection-name",
    "mongodb-cluster-name": "Cluster0",
    "mongodb-connection-name": "mongodb-sink-connection",
    "mongodb-group-id": "your-24-char-mongodb-group-id",
    "mongodb-tenant-name": "your-stream-instance-name"
}
```

**Example** (`config.json`):
```json
{
    "confluent-cluster-id": "lkc-abc123",
    "confluent-rest-endpoint": "https://pkc-xyz789.us-west-2.aws.confluent.cloud:443",
    "mongodb-stream-processor-instance-url": "mongodb://atlas-stream-67d97a1d8820c02d373b6f28-gkmrk.virginia-usa.a.query.mongodb.net/",
    "stream-processor-prefix": "managed-sink-test",
    "kafka-connection-name": "kafka-connection-name",
    "mongodb-cluster-name": "Cluster0",
    "mongodb-connection-name": "mongodb-sink-connection",
    "mongodb-group-id": "507f1f77bcf86cd799439011",
    "mongodb-tenant-name": "my-stream-instance"
}
```

### Connector Configuration Files

Create individual JSON files for each connector. Each file should contain:

```json
{
    "kafka.api.key": "your-kafka-api-key",
    "kafka.api.secret": "your-kafka-api-secret",
    "input.data.format": "JSON",
    "connection.user": "dbuser",
    "connection.password": "dbpassword",
    "topics": "single-topic-name",
    "database": "db_name",
    "collection": "collection_name",
    "consumer.override.auto.offset.reset": "earliest"
}
```

**Required fields**:
- `kafka.api.key`: API key for Confluent authentication
- `kafka.api.secret`: API secret for Confluent authentication
- `input.data.format`: Data format (typically "JSON")
- `connection.user`: MongoDB database username for stream processor authentication
- `connection.password`: MongoDB database password for stream processor authentication
- `topics`: Topic name(s) to consume from (can be a string or array)
- `database`: Database name
- `collection`: Collection name

**Optional fields**:
- `consumer.override.auto.offset.reset`: Kafka consumer offset reset behavior ("earliest" or "latest")

**Topics Field Examples**:
```json
// Single topic
{
    "topics": "my-topic"
}

// Multiple topics
{
    "topics": ["topic1", "topic2", "topic3"]
}
```

## Usage

### Basic Usage

```bash
python create_sink_processors.py <config.json> <connector_configs_folder>
```

### Examples

**Example 1: Basic usage**
```bash
python create_sink_processors.py config.json ./topics/
```

**Example 2: With absolute paths**
```bash
python create_sink_processors.py /path/to/config.json /path/to/topics/
```

**Example 3: Current directory**
```bash
python create_sink_processors.py config.json .
```

## Project Structure

```
mongodb-sink-stream-processor-setup/
├── create_sink_processors.py     # Main application
├── requirements.txt               # Python dependencies
├── README.md                     # This file
├── config.json                   # Main configuration (create this)
└── topics/                       # Folder with connector configs (create this)
    ├── connector1.json
    ├── connector2.json
    └── connector3.json
```

## Complete Pipeline Configuration

The tool creates a complete Kafka → MongoDB streaming pipeline with the following components:

### 1. MongoDB Atlas Stream Processing Kafka Connection
- **Type**: Kafka connection
- **Authentication**: SASL/PLAIN using API key/secret from connector configs
- **Security**: SASL_SSL protocol
- **Bootstrap Servers**: Derived from Confluent REST endpoint

### 2. MongoDB Atlas Stream Processing Sink Connection
- **Type**: Cluster connection
- **Target**: MongoDB Atlas cluster specified in main config
- **Authentication**: Uses Atlas CLI authentication
- **Database Role**: `readWriteAnyDatabase` (built-in role)

### 3. Stream Processors
- **Naming Convention**: `<stream-processor-prefix>_<database>_<collection>`
- **Pipeline**: Source from Kafka topics → Merge into MongoDB collection
- **Authentication**: Uses `connection.user` and `connection.password` from connector configs
- **Auto Offset Reset**: Configurable via `consumer.override.auto.offset.reset`

### 4. MongoDB Collections
- **Creation**: Collections are created automatically when data is first inserted
- **No manual creation needed**: MongoDB handles collection creation dynamically

### Example
If your connector config has:
- `topics`: "my-app.events.user-actions"
- `database`: "analytics"
- `collection`: "user_events"
- `stream-processor-prefix`: "managed-sink-test"

The tool will create:
- **Kafka Connection**: `kafka-connection-name` (shared)
- **MongoDB Sink Connection**: `mongodb-sink-connection` (shared)
- **Stream Processor**: `managed-sink-test_analytics_user_events`
- **MongoDB Collection**: `analytics.user_events` (created automatically)

## Output Examples

### Successful Run
```
Loading main configuration...
✓ Main config loaded successfully
  Confluent Cluster ID: lkc-abc123
  Confluent REST Endpoint: https://pkc-xyz789.us-west-2.aws.confluent.cloud:443
  Stream Processor URL: mongodb://atlas-stream-67d97a1d8820c02d373b6f28-gkmrk.virginia-usa.a.query.mongodb.net/
  Stream Processor Prefix: managed-sink-test
  Kafka Connection Name: kafka-connection-name
  MongoDB Connection Name: mongodb-sink-connection

Found 3 .json files to process
--------------------------------------------------

Creating MongoDB sink connection: mongodb-sink-connection
✓ Successfully created MongoDB sink connection: mongodb-sink-connection

Creating Kafka connection: kafka-connection-name
✓ Successfully created Kafka connection: kafka-connection-name

Processing: user-events.json
Creating stream processor: managed-sink-test_analytics_user_events
✓ Successfully created stream processor: managed-sink-test_analytics_user_events

Processing: product-views.json
Creating stream processor: managed-sink-test_analytics_product_views
✓ Successfully created stream processor: managed-sink-test_analytics_product_views

Processing: orders.json
⚠ Stream processor already exists: managed-sink-test_ecommerce_orders
--------------------------------------------------
Summary:
  MongoDB sink connection: 1/1 created
  Kafka connection: 1/1 created
  Stream processors: 3/3 created successfully
```

### Error Handling
```
Processing: bad_connector.json
Error: Missing required field 'connection.user' in bad_connector.json

Processing: invalid_offset.json
✗ Error: auto offset reset must be 'earliest' or 'latest', got 'invalid' in invalid_offset.json

Processing: auth_error.json
Creating stream processor: managed-sink-test_test_data
✗ Failed to create stream processor managed-sink-test_test_data
  Error: MongoServerError: authentication failed
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

3. **MongoDB Atlas CLI authentication errors**:
   - Ensure you're authenticated with Atlas CLI: `atlas auth login`
   - Verify your Atlas CLI user has Stream Processing Owner role
   - Check that the group ID and tenant name are correct

4. **Stream processor authentication errors**:
   - Verify the `connection.user` and `connection.password` in connector configs
   - Ensure the database user has appropriate read/write permissions
   - Check that the MongoDB stream processor instance URL is correct

5. **mongosh connection errors**:
   - Verify mongosh is installed and in PATH
   - Check the stream processor instance URL format
   - Ensure the stream processing instance exists and is active

6. **Network issues**:
   - Ensure you have internet connectivity
   - Check if the REST endpoint URLs are correct
   - Verify firewall settings for both Confluent and MongoDB Atlas

7. **Connections/processors already exist**:
   - This is normal behavior and will be logged as a warning
   - The script will continue processing other items

8. **Invalid auto offset reset values**:
   - Ensure `consumer.override.auto.offset.reset` is either "earliest" or "latest"
   - This field is optional and can be omitted

### Debug Mode

For more verbose output, you can modify the script or add print statements to see detailed request/response information.

## API References

The tool uses the following APIs:

**MongoDB Atlas CLI**:
```
atlas streams connections create <connection_name> --projectId <groupId> --instance <tenantName>
```

**MongoDB Shell (mongosh)**:
```
sp.createStreamProcessor(<name>, <pipeline>)
```

For more information, see:
- [MongoDB Atlas CLI documentation](https://www.mongodb.com/docs/atlas/cli/stable/)
- [MongoDB Atlas Stream Processing documentation](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)
- [Confluent REST API documentation](https://docs.confluent.io/platform/current/kafka-rest/api.html)

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is provided as-is for educational and development purposes.

---

**Note**: This tool creates a complete end-to-end streaming pipeline from Kafka to MongoDB using MongoDB Atlas Stream Processing. Make sure you have the necessary permissions and authentication set up for all services (Confluent Cloud, MongoDB Atlas, Atlas CLI, and mongosh) before running the script.