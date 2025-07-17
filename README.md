# MongoDB Atlas Stream Processing Setup Tools

This project provides automated setup tools for creating end-to-end streaming pipelines between MongoDB and Apache Kafka using MongoDB Atlas Stream Processing. The tools are designed to work with familiar Confluent MongoDB connector configuration formats, making it easy to migrate from traditional connector-based architectures to MongoDB's native stream processing platform.

## Overview

The project consists of two complementary tools that handle different streaming directions:

- **[MongoDB Source](./mongodb_source/)**: Streams data from MongoDB collections to Kafka topics
- **[MongoDB Sink](./mongodb_sink/)**: Streams data from Kafka topics to MongoDB collections

Both tools use the same underlying infrastructure and share connection configurations, making it easy to create bidirectional streaming pipelines.

## Architecture

```
┌─────────────────┐    ┌─────────────────────┐    ┌─────────────────┐
│   MongoDB       │    │   MongoDB Atlas     │    │   Apache Kafka  │
│   Collections   │◄──►│ Stream Processing   │◄──►│     Topics      │
│                 │    │                     │    │                 │
└─────────────────┘    └─────────────────────┘    └─────────────────┘
```

### Key Components

1. **MongoDB Atlas Stream Processing Instance**: Central processing engine that runs the stream processors
2. **Connections**: Reusable connection configurations for MongoDB clusters and Kafka clusters
3. **Stream Processors**: Individual processing units that handle data transformation and routing
4. **Configuration Files**: JSON files that define the streaming requirements in familiar Confluent format

## Features

- **Bidirectional Streaming**: Support for both MongoDB → Kafka and Kafka → MongoDB data flows
- **Familiar Configuration**: Uses Confluent MongoDB connector configuration format
- **Automated Setup**: Creates all necessary connections, topics, and stream processors automatically
- **Bulk Operations**: Process multiple collections/topics in a single command
- **Error Handling**: Comprehensive error handling with clear logging
- **Reusable Connections**: Share connection configurations across multiple stream processors
- **Flexible Topic Handling**: Support for single topics or multiple topics per stream processor

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
   pip install requests
   ```

### Quick Start

1. **Configure your main settings** in `config.json` files (see tool-specific documentation)
2. **Create connector configuration files** for each collection/topic you want to process
3. **Run the appropriate tool**:
   ```bash
   # For MongoDB → Kafka streaming
   cd mongodb_source
   python create_source_processors.py config.json ./sources/
   
   # For Kafka → MongoDB streaming
   cd mongodb_sink
   python create_sink_processors.py config.json ./topics/
   ```

## Tools

### MongoDB Source Tool

**Purpose**: Stream data from MongoDB collections to Kafka topics

**Key Features**:
- Creates Kafka topics automatically
- Sets up change stream monitoring on MongoDB collections
- Handles authentication and connection management
- Supports bulk collection processing

**[📖 Full Documentation](./mongodb_source/README.md)**

### MongoDB Sink Tool

**Purpose**: Stream data from Kafka topics to MongoDB collections

**Key Features**:
- Supports single or multiple topics per collection
- Configurable offset reset behavior
- Automatic collection creation
- Handles authentication and connection management

**[📖 Full Documentation](./mongodb_sink/README.md)**

## Configuration

### Main Configuration File

Both tools use a shared configuration format:

```json
{
    "confluent-cluster-id": "lkc-abc123",
    "confluent-rest-endpoint": "https://pkc-xyz789.us-west-2.aws.confluent.cloud:443",
    "mongodb-stream-processor-instance-url": "mongodb://atlas-stream-instance-url/",
    "stream-processor-prefix": "my-app",
    "kafka-connection-name": "kafka-connection-name",
    "mongodb-connection-name": "mongodb-connection-name",
    "mongodb-cluster-name": "Cluster0",
    "mongodb-group-id": "your-24-char-mongodb-group-id",
    "mongodb-tenant-name": "your-stream-instance-name"
}
```

### Connector Configuration Files

Each tool uses connector-specific configuration files that follow familiar Confluent formats:

- **Source configurations**: Similar to MongoDB Atlas Source Connector format
- **Sink configurations**: Similar to MongoDB Atlas Sink Connector format

See the individual tool documentation for detailed configuration examples.

## Project Structure

```
confluent_config_to_asp/
├── README.md                     # This file
├── mongodb_source/               # MongoDB → Kafka streaming tool
│   ├── README.md
│   ├── create_source_processors.py
│   ├── config.json
│   └── sources/
│       ├── collection1.json
│       └── collection2.json
└── mongodb_sink/                 # Kafka → MongoDB streaming tool
    ├── README.md
    ├── create_sink_processors.py
    ├── config.json
    └── topics/
        ├── topic1.json
        └── topic2.json
```

## Common Workflows

### Setting Up Bidirectional Streaming

1. **Configure shared settings** in both `mongodb_source/config.json` and `mongodb_sink/config.json`
2. **Create source configurations** for collections you want to stream to Kafka
3. **Create sink configurations** for topics you want to stream to MongoDB
4. **Run both tools**:
   ```bash
   # Set up MongoDB → Kafka streaming
   cd mongodb_source
   python create_source_processors.py config.json ./sources/
   
   # Set up Kafka → MongoDB streaming
   cd mongodb_sink
   python create_sink_processors.py config.json ./topics/
   ```

### Migrating from Confluent Connectors

1. **Export your existing connector configurations**
2. **Adapt the configurations** to match the tool formats (minimal changes required)
3. **Run the appropriate tool** to create equivalent stream processors
4. **Test and validate** the streaming pipeline
5. **Decommission old connectors** once validation is complete

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

For tool-specific issues, consult the individual README files:
- [MongoDB Source Troubleshooting](./mongodb_source/README.md#troubleshooting)
- [MongoDB Sink Troubleshooting](./mongodb_sink/README.md#troubleshooting)

## API References

The tools interact with the following APIs:

- **[MongoDB Atlas CLI](https://www.mongodb.com/docs/atlas/cli/stable/)**: Connection and stream processor management
- **[MongoDB Shell (mongosh)](https://www.mongodb.com/docs/mongodb-shell/)**: Stream processor creation and management
- **[Confluent REST API](https://docs.confluent.io/platform/current/kafka-rest/api.html)**: Topic management (source tool only)
- **[MongoDB Atlas Stream Processing](https://www.mongodb.com/docs/atlas/atlas-stream-processing/)**: Core streaming platform

## Contributing

Feel free to submit issues and enhancement requests! When contributing:

1. Test changes with both source and sink tools
2. Update relevant documentation
3. Ensure backward compatibility with existing configurations
4. Add appropriate error handling

## License

This project is provided as-is for educational and development purposes.

---

**Note**: These tools create production-ready streaming pipelines. Ensure you have proper monitoring, alerting, and backup strategies in place before using in production environments.