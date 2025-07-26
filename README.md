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

1. **Configure your main settings** in `config.json` files (see tool-specific documentation)
2. **Create connector configuration files** for each collection/topic you want to process.

**For Sources**
```
 {
     "connector.class": "MongoDbAtlasSource",
     "name": "<my-stream-processor-name>",
     "kafka.auth.mode": "KAFKA_API_KEY",
     "kafka.api.key": "<my-kafka-api-key>",
     "kafka.api.secret": "<my-kafka-api-secret>",
     "topic.prefix": "<topic-prefix>",
     "connection.user": "<database-username>",
     "connection.password": "<database-password>",
     "database": "<database-name>",
     "collection": "<database-collection-name>",
}
```

**For Sinks**
```
{
    "connector.class": "MongoDbAtlasSink",
    "name": "confluent-mongodb-sink",
    "kafka.auth.mode": "KAFKA_API_KEY",
    "kafka.api.key": "<my-kafka-api-key",
    "kafka.api.secret": "<my-kafka-api-secret>",
    "connection.user": "<my-username>",
    "connection.password": "<my-password>",
    "topics": "<comma-separated-topics>",
    "database": "<database-name>",
    "collection": "<collection-name>",
}
```

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
    "connector.class": "com.mongodb.kafka.connect.MongoSourceConnector",
    "name": "my-source-connector",
    "kafka.auth.mode": "KAFKA_API_KEY",  # ✅ Supported value
    "database": "mydb",                   # ✅ Required field
    "timeseries.field": "timestamp"       # ❌ Unsupported field
}
```

If validation fails, you'll get clear error messages like:
- `Missing required fields: database, collection`
- `The following fields are not supported: timeseries.field`
- `Only KAFKA_API_KEY is supported for kafka.auth.mode`

3. **Run the appropriate tool**:
   ```bash
   # For MongoDB → Kafka streaming
   python3 create_source_processors.py <main_config_file.json> <folder_with_source_configs/>
   
   # For Kafka → MongoDB streaming
   python3 create_sink_processors.py <main_config_file.json> <folder_with_sink_configs/>
   ```

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