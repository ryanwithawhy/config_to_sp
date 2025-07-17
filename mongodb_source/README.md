# MongoDB to Kafka Stream Processor Setup Tool

This tool automatically sets up end-to-end streaming of your MongoDB collection change streams to Apache Kafka topics using the MongoDB-Kafka configuration files you're already familiar with.  All you need to do is create the stream processing instance that will run your processors and, once your processors are created, press start.

## Overview

This tool takes a [main configuration file](#main-configuration-file) with some information about your MongoDB Atlas cluster and stream processing instance and a [configuration file per collection](#connector-configuration-files) in the Confluent MongoDB Atlas Source Connector [format](https://docs.confluent.io/cloud/current/connectors/cc-mongo-db-source).  It then processes the files to create:
1. **Kafka topics** with the naming convention `<topic.prefix>.<database>.<collection>`
2. **MongoDB Atlas Stream Processing source connections** to read from MongoDB clusters
3. **MongoDB Atlas Stream Processing Kafka connections** to write to Confluent Cloud
4. **Stream processors** that automatically stream data from MongoDB collections to Kafka topics

## Features

- **Complete pipeline setup**: Creates the entire MongoDB → Kafka streaming pipeline
- **Bulk stream processor setup**: Create stream processors for multiple collections in a single command
- **Automatic naming**: Uses consistent naming conventions across all components
- **Error handling**: Comprehensive error handling with clear logging
- **Progress tracking**: Shows success/failure status for each creation step
- **Duplicate handling**: Gracefully handles existing topics, connections, and processors
- **JSON validation**: Validates configuration files before processing

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
    "stream-processor-prefix": "kafka-stream",
    "kafka-connection-name": "kafka-connection-name",
    "mongodb-cluster-name": "Cluster0",
    "mongodb-connection-name": "mongodb-source-connection",
    "mongodb-group-id": "your-24-char-mongodb-group-id",
    "mongodb-tenant-name": "your-stream-instance-name"
}
```

**Example** (`main_config.json`):
```json
{
    "confluent-cluster-id": "lkc-abc123",
    "confluent-rest-endpoint": "https://pkc-xyz789.us-west-2.aws.confluent.cloud:443",
    "mongodb-stream-processor-instance-url": "mongodb://atlas-stream-67d97a1d8820c02d373b6f28-gkmrk.virginia-usa.a.query.mongodb.net/",
    "stream-processor-prefix": "kafka-consumer",
    "kafka-connection-name": "kafka-connection-name",
    "mongodb-cluster-name": "Cluster0",
    "mongodb-connection-name": "mongodb-source-connection",
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
    "topic.prefix": "mongodb",
    "connection.user": "dbuser",
    "connection.password": "dbpassword",
    "database": "db_name",
    "collection": "collection_name",
}
```

**Required fields**:
- `kafka.api.key`: API key for Confluent authentication
- `kafka.api.secret`: API secret for Confluent authentication
- `topic.prefix`: Prefix for the topic name
- `database`: Database name
- `collection`: Collection name
- `connection.user`: MongoDB database username for stream processor authentication
- `connection.password`: MongoDB database password for stream processor authentication

## Usage

### Basic Usage

```bash
python create_source_processors.py <main_config.json> <connector_configs_folder>
```

### Examples

**Example 1: Basic usage**
```bash
python create_source_processors.py main_config.json ./connector_configs/
```

**Example 2: With absolute paths**
```bash
python create_source_processors.py /path/to/main_config.json /path/to/connector_configs/
```

**Example 3: Current directory**
```bash
python create_source_processors.py config.json .
```

## Project Structure

```
mongodb-stream-processor-setup/
├── create_source_processors.py    # Main application
├── requirements.txt                # Python dependencies
├── README.md                      # This file
├── config.json                    # Main configuration (create this)
└── sources/                       # Folder with connector configs (create this)
    ├── connector1.json
    ├── connector2.json
    └── connector3.json
```

## Complete Pipeline Configuration

The tool creates a complete MongoDB → Kafka streaming pipeline with the following components:

### 1. Kafka Topics
- **Partitions**: 3
- **Cleanup Policy**: delete
- **Naming Convention**: `<topic.prefix>.<database>.<collection>`

### 2. MongoDB Atlas Stream Processing Source Connection
- **Type**: Cluster connection
- **Target**: MongoDB Atlas cluster specified in main config
- **Authentication**: Uses Atlas CLI authentication
- **Database Role**: `readAnyDatabase` (built-in role)

### 3. MongoDB Atlas Stream Processing Kafka Connection
- **Type**: Kafka connection
- **Authentication**: SASL/PLAIN using API key/secret from connector configs
- **Security**: SASL_SSL protocol
- **Bootstrap Servers**: Derived from Confluent REST endpoint

### 4. Stream Processors
- **Naming Convention**: `<stream-processor-prefix>_<database>_<collection>`
- **Pipeline**: Source from MongoDB → Emit to Kafka topic
- **Authentication**: Uses `connection.user` and `connection.password` from connector configs

### Example
If your connector config has:
- `topic.prefix`: "mongodb"
- `database`: "inventory"
- `collection`: "products"
- `stream-processor-prefix`: "kafka-consumer"

The tool will create:
- **Kafka Topic**: `mongodb.inventory.products`
- **MongoDB Source Connection**: `mongodb-source-connection` (shared)
- **Kafka Connection**: `kafka-connection-name` (shared)
- **Stream Processor**: `kafka-consumer_inventory_products`

## Output Examples

### Successful Run
```
Loading main configuration...
✓ Main config loaded successfully
  Confluent Cluster ID: lkc-abc123
  Confluent REST Endpoint: https://pkc-xyz789.us-west-2.aws.confluent.cloud:443
  Stream Processor URL: mongodb://atlas-stream-67d97a1d8820c02d373b6f28-gkmrk.virginia-usa.a.query.mongodb.net/
  Stream Processor Prefix: kafka-consumer
  Kafka Connection Name: kafka-connection-name
  MongoDB Source Cluster Name: Cluster0
  MongoDB Source Connection Name: mongodb-source-connection

Found 3 .json files to process
--------------------------------------------------

Creating shared MongoDB source connection: mongodb-source-connection
✓ Successfully created MongoDB source connection: mongodb-source-connection

Creating shared Kafka connection: kafka-connection-name
✓ Successfully created Kafka connection: kafka-connection-name

Processing: connector1.json
✓ Successfully created topic: mongodb.inventory.products
✓ Using existing Kafka connection: kafka-connection-name
Creating stream processor: kafka-consumer_inventory_products
✓ Successfully created stream processor: kafka-consumer_inventory_products

Processing: connector2.json
✓ Successfully created topic: mongodb.inventory.orders
✓ Using existing Kafka connection: kafka-connection-name
Creating stream processor: kafka-consumer_inventory_orders
✓ Successfully created stream processor: kafka-consumer_inventory_orders

Processing: connector3.json
⚠ Topic already exists: mongodb.users.profiles
✓ Using existing Kafka connection: kafka-connection-name
Creating stream processor: kafka-consumer_users_profiles
✓ Successfully created stream processor: kafka-consumer_users_profiles
--------------------------------------------------
Summary:
  Kafka topics: 3/3 created successfully
  MongoDB source connection: 1/1 created
  Kafka connection: 1/1 created
  Stream processors: 3/3 created successfully
```

### Error Handling
```
Processing: bad_connector.json
Error: Missing required field 'connection.user' in bad_connector.json

Processing: network_issue.json
✗ Network error creating topic mongodb.test.collection: Connection timeout

Processing: auth_error.json
✓ Successfully created topic: mongodb.test.data
✓ Using existing Kafka connection: kafka-connection-name
Creating stream processor: kafka-consumer_test_data
✗ Failed to create stream processor kafka-consumer_test_data
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
   - Ensure the database user has appropriate read permissions
   - Check that the MongoDB stream processor instance URL is correct

5. **mongosh connection errors**:
   - Verify mongosh is installed and in PATH
   - Check the stream processor instance URL format
   - Ensure the stream processing instance exists and is active

6. **Network issues**:
   - Ensure you have internet connectivity
   - Check if the REST endpoint URLs are correct
   - Verify firewall settings for both Confluent and MongoDB Atlas

7. **Topics/connections/processors already exist**:
   - This is normal behavior and will be logged as a warning
   - The script will continue processing other items

8. **Missing MongoDB configuration**:
   - Ensure mongodb-group-id and mongodb-tenant-name are in your main config
   - Without these, only Kafka topics will be created (connections and processors will be skipped)

### Debug Mode

For more verbose output, you can modify the script or add print statements to see detailed request/response information.

## API References

The tool uses the following APIs:

**Confluent REST API v3**:
```
POST /kafka/v3/clusters/{cluster_id}/topics
```

**MongoDB Atlas CLI**:
```
atlas streams connections create <connection_name> --projectId <groupId> --instance <tenantName>
```

**MongoDB Shell (mongosh)**:
```
sp.createStreamProcessor(<name>, <pipeline>)
```

For more information, see:
- [Confluent REST API documentation](https://docs.confluent.io/platform/current/kafka-rest/api.html)
- [MongoDB Atlas CLI documentation](https://www.mongodb.com/docs/atlas/cli/stable/)
- [MongoDB Atlas Stream Processing documentation](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)

## Contributing

Feel free to submit issues and enhancement requests!

## License

This project is provided as-is for educational and development purposes.

---

**Note**: This tool creates a complete end-to-end streaming pipeline from MongoDB to Kafka using MongoDB Atlas Stream Processing. Make sure you have the necessary permissions and authentication set up for all services (Confluent Cloud, MongoDB Atlas, Atlas CLI, and mongosh) before running the script.