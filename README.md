# MongoDB Atlas Stream Processing Setup Tools

This project provides automated setup tools for creating end-to-end streaming pipelines between MongoDB clusters on Atlas and Apache Kafka topics using Atlas Stream Processing. This tools is designed to work with the configuration files used to setup Confluent's managed MongoDB connector.

## Overview

To use this project you simply create a single configuration scripe with a few MongoDB Atlas-specific fields, and then run `python create_processors.py {main_config_file} {stream_processor_config_location/}`.  It will then will automatically create stream processors that you can start running to stream data from MongoDB Atlas clusters to Kafka topics or the reverse.  

## Architecture

```
┌─────────────────┐    ┌─────────────────────┐    ┌─────────────────┐
│   MongoDB       │    │   MongoDB Atlas     │    │   Apache Kafka  │
│   Collections   │◄──►│ Stream Processing   │◄──►│     Topics      │
│                 │    │                     │    │                 │
└─────────────────┘    └─────────────────────┘    └─────────────────┘
```

## Getting Started

### Prerequisites

- Python 3.6 or higher
- MongoDB Shell (mongosh) installed and in PATH
- MongoDB Atlas CLI installed and authenticated (`atlas auth login`)
- Internet connection for API calls to Confluent Cloud and MongoDB Atlas

### Installation

1. **Clone the project**:
   ```bash
   git clone <repository-url>
   cd confluent_config_to_asp
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### Quick Start

1. **Configure your main settings** in a main configuration file (see [Main Configuration](#main-configuration) below)
2. **Create connector configuration files** for each collection/topic you want to process (see [Connector Configurations](#connector-configurations) below)
3. **Run the appropriate tool**:
   ```bash
   # For MongoDB → Kafka streaming
   python3 create_source_processors.py <main_config_file.json> <folder_with_source_configs/>
   
   # For Kafka → MongoDB streaming
   python3 create_sink_processors.py <main_config_file.json> <folder_with_sink_configs/>
   ```

## Main Configuration

The main configuration file contains settings for your MongoDB Atlas and Confluent Cloud environments. This file is passed as the first argument to the processing scripts.

### Required Fields

```json
{
    "confluent-cluster-id": "lkc-12345",
    "confluent-rest-endpoint": "https://pkc-abc123.us-west-2.aws.confluent.cloud:443",
    "mongodb-stream-processor-instance-url": "mongodb://cluster0-shard-00-00.mongodb.net:27017",
    "stream-processor-prefix": "myapp",
    "kafka-connection-name": "kafka-connection",
    "mongodb-connection-name": "mongodb-connection", 
    "mongodb-cluster-name": "Cluster0",
    "mongodb-group-id": "507f1f77bcf86cd799439011",
    "mongodb-tenant-name": "MyTenant",
    "mongodb-connection-role": "readWrite"
}
```

### Field Descriptions

| Field | Description | Example |
|-------|-------------|---------|
| `confluent-cluster-id` | Your Confluent Cloud Kafka cluster ID | `"lkc-12345"` |
| `confluent-rest-endpoint` | Confluent Cloud REST API endpoint | `"https://pkc-abc123.us-west-2.aws.confluent.cloud:443"` |
| `mongodb-stream-processor-instance-url` | MongoDB Atlas Stream Processing instance URL | `"mongodb://cluster0-shard-00-00.mongodb.net:27017"` |
| `stream-processor-prefix` | Prefix for naming stream processors | `"myapp"` |
| `kafka-connection-name` | Name for the shared Kafka connection | `"kafka-connection"` |
| `mongodb-connection-name` | Name for the shared MongoDB connection | `"mongodb-connection"` |
| `mongodb-cluster-name` | Name of your MongoDB Atlas cluster | `"Cluster0"` |
| `mongodb-group-id` | MongoDB Atlas project/group ID | `"507f1f77bcf86cd799439011"` |
| `mongodb-tenant-name` | MongoDB Atlas organization/tenant name | `"MyTenant"` |
| `mongodb-connection-role` | Database role for MongoDB connection | `"readWrite"` |

### How to Find These Values

#### Confluent Cloud Values
- **Cluster ID**: Found in Confluent Cloud console under Cluster Settings
- **REST Endpoint**: Found in Cluster Settings → Endpoints section

#### MongoDB Atlas Values  
- **Stream Processor URL**: Found in Atlas Stream Processing section
- **Cluster Name**: Your Atlas cluster name (e.g., "Cluster0")
- **Group ID**: Found in Project Settings → General
- **Tenant Name**: Your Atlas organization name
- **Connection Role**: Database user role (typically "readWrite" or "readWriteAnyDatabase")

### Example Main Config

```json
{
    "confluent-cluster-id": "lkc-abc123", 
    "confluent-rest-endpoint": "https://pkc-xyz789.us-east-1.aws.confluent.cloud:443",
    "mongodb-stream-processor-instance-url": "mongodb://myapp-shard-00-00.abc123.mongodb.net:27017",
    "stream-processor-prefix": "ecommerce",
    "kafka-connection-name": "ecommerce-kafka-conn",
    "mongodb-connection-name": "ecommerce-mongo-conn",
    "mongodb-cluster-name": "EcommerceCluster", 
    "mongodb-group-id": "60f1b2c3d4e5f6789a0b1c2d",
    "mongodb-tenant-name": "AcmeCorpAtlas",
    "mongodb-connection-role": "readWrite"
}
```

## Connector Configurations

Create individual JSON configuration files for each source or sink connector you want to deploy. These follow the standard Confluent connector format but are validated for MongoDB Atlas Stream Processing compatibility.

### Source Connector Configuration

**For Sources (MongoDB → Kafka)**
```json
{
    "connector.class": "MongoDbAtlasSource",
    "name": "orders-source-processor",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "your-kafka-api-key",
    "kafka.api.secret": "your-kafka-api-secret",
    "topic.prefix": "ecommerce",
    "connection.user": "dbuser",
    "connection.password": "dbpassword",
    "database": "orders",
    "collection": "transactions"
}
```

**Note**: The `collection` field is **optional** for source connectors. If omitted, the stream processor will watch all collections in the specified database.

### Sink Connector Configuration

**For Sinks (Kafka → MongoDB)**
```json
{
    "connector.class": "MongoDbAtlasSink",
    "name": "orders-sink-processor",
    "kafka.auth.mode": "KAFKA_API_KEY", 
    "kafka.api.key": "your-kafka-api-key",
    "kafka.api.secret": "your-kafka-api-secret",
    "connection.user": "dbuser",
    "connection.password": "dbpassword",
    "topics": "ecommerce.orders.transactions",
    "database": "orders",
    "collection": "transactions"
}
```

**Note**: The `collection` field is **required** for sink connectors.

## Configuration Validation

The tools include automatic configuration validation to ensure your connector configurations are compatible with MongoDB Atlas Stream Processing.

### How It Works

1. **Auto-Detection**: The system automatically detects whether your configuration is for a source or sink connector by examining the `connector.class` field
2. **Comprehensive Validation**: Validates all configuration properties against MongoDB Atlas Stream Processing requirements
3. **Clear Error Messages**: Provides specific guidance on configuration issues

### Validation Types

- **Required Fields**: Must be present (e.g., `name`, `connector.class`, `database`)
- **Unsupported Fields**: Will cause validation to fail if present (e.g., certain timeseries parameters)
- **Restricted Values**: Only specific values are allowed (e.g., `kafka.auth.mode` must be `KAFKA_API_KEY`)

### Example Validation

```python
# Configuration is automatically validated
{
    "connector.class": "MongoSourceConnector",
    "name": "my-source-connector",
    "kafka.auth.mode": "KAFKA_API_KEY",  # ✅ Supported value
    "database": "mydb",                   # ✅ Required field
    "timeseries.field": "timestamp"       # ❌ Unsupported field
}
```

If validation fails, you'll get clear error messages like:
- `Missing required fields: database, topic.prefix` (for sources)
- `Missing required fields: database, collection, topics` (for sinks) 
- `The following fields are not supported: timeseries.field`
- `Only KAFKA_API_KEY is supported for kafka.auth.mode`

## Authentication & Permissions

### MongoDB Atlas

- **Atlas CLI**: Must be authenticated with `atlas auth login`
- **Database User**: Requires appropriate read/write permissions for target databases
- **Stream Processing**: User must have Stream Processing Owner role

### Confluent Cloud

- **API Keys**: Required for Kafka cluster access
- **REST API**: Used for topic management (source tool only)

## Troubleshooting

### Common Issues

1. **Authentication Errors**:
   - Verify Atlas CLI authentication: `atlas auth whoami`
   - Check database user permissions
   - Validate API key/secret pairs

2. **Connection Issues**:
   - Verify stream processor instance URL format
   - Check network connectivity to both MongoDB Atlas and Confluent Cloud
   - Validate cluster IDs and endpoints

3. **Configuration Errors**:
   - Validate JSON syntax in configuration files
   - Ensure all required fields are present
   - Check field value formats and constraints

### Getting Help

For script-specific issues:
- Check the script output for detailed error messages
- Run unit tests to verify your environment: `cd tests && python3 run_tests.py --unit-only`
- Use verbose mode for more detailed logging: `python3 create_source_processors.py config.json sources/ -v`

For detailed technical information, see [`docs/DEVELOPER.md`](docs/DEVELOPER.md).

## API References

The tools interact with the following APIs:

- **[MongoDB Atlas CLI](https://www.mongodb.com/docs/atlas/cli/stable/)**: Connection and stream processor management
- **[MongoDB Shell (mongosh)](https://www.mongodb.com/docs/mongodb-shell/)**: Stream processor creation and management
- **[Confluent REST API](https://docs.confluent.io/platform/current/kafka-rest/api.html)**: Topic management (source tool only)
- **[MongoDB Atlas Stream Processing](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)**: Core streaming platform

## Contributing

Feel free to submit issues and enhancement requests! When contributing:

1. **Run the test suite** to ensure your changes don't break existing functionality:
   ```bash
   cd tests && python3 run_tests.py -v
   ```
2. **Add tests** for new functionality in the `tests/unit/` or `tests/integration/` directories
3. **Test changes** with both source and sink scripts
4. **Update relevant documentation** (both user and developer docs)
5. **Ensure backward compatibility** with existing configurations

For detailed development guidelines, see [`docs/DEVELOPER.md`](docs/DEVELOPER.md).

## License

This project is provided as-is for educational and development purposes.

---

**Note**: These tools create production-ready streaming pipelines. Ensure you have proper monitoring, alerting, and backup strategies in place before using in production environments.