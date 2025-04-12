# Retail Data Simulator Kafka Connector

[![Build Status](https://github.com/your-org/retail-data-simulator-connector/workflows/Build/badge.svg)](https://github.com/your-org/retail-data-simulator-connector/actions)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

A Kafka Connect source connector that simulates retail data events for inventory, orders, and logistics. This connector generates realistic retail data streams for demonstration and testing purposes, particularly useful for RAG (Retrieval-Augmented Generation) and event automation demos.

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
  - [Building the Connector](#building-the-connector)
  - [Running with Docker](#running-with-docker)
  - [Manual Installation](#manual-installation)
- [Configuration](#configuration)
  - [Connector Configuration](#connector-configuration)
- [Data Schemas](#data-schemas)
- [Development](#development)
  - [Customizing Data Generation](#customizing-data-generation)
  - [Testing](#testing)
- [Deployment](#deployment)
- [Monitoring](#monitoring)
- [Contributing](#contributing)
- [License](#license)

## Overview

This connector generates three types of retail events:

1. **Inventory Events**: Simulates inventory updates with SKUs, quantities, and reorder thresholds
2. **Order Events**: Simulates customer orders with items, payment details, and loyalty points
3. **Logistics Events**: Simulates shipment updates with carrier information, status, and cost components

The connector is designed to work with Avro schemas and can integrate with schema registries.

## Key Features

- Generates realistic retail data streams for demos and testing
- Supports multiple event types (inventory, orders, logistics)
- Configurable event generation frequency and batch size
- Proper schema definitions for all event types
- Offset tracking for consistent data generation
- Easy integration with Kafka Connect ecosystem

## Project Structure

```shell
├── src/main/java/
│   └── com/retaildatasimulator/kafka/connect/
│       ├── source/
│       │   ├── RetailDataSimulatorConnector.java  # Main connector class
│       │   ├── RetailDataSimulatorTask.java       # Task implementation
│       │   └── RetailDataSimulatorConfig.java     # Configuration class
├── src/test/                  # Test code
├── config/                    # Example configurations
│   ├── json/
│   │   └── source-connector.json  # Example connector config
└── docker/                    # Docker development setup
```

## Prerequisites

- Java 11+
- Maven 3.6.3+
- Docker and Docker Compose (for local development)
- Apache Kafka 3.x+ (or Confluent Platform)
- Schema Registry (optional)

## Getting Started

### Building the Connector

```bash
git clone https://github.com/your-org/retail-data-simulator-connector.git
cd retail-data-simulator-connector
mvn clean package
```

The build will produce a fat JAR in `target/` containing all dependencies.

### Running with Docker

A complete development environment is provided:

```bash
cd docker
docker-compose up -d
```

This starts:

- Zookeeper
- Kafka broker
- Kafka Connect with the connector pre-installed
- Schema Registry (optional)

### Manual Installation

1. Build the connector JAR
2. Copy to Kafka Connect plugins directory:

   ```bash
   cp target/retail-data-simulator-connector-*.jar $KAFKA_CONNECT_PLUGINS_DIR/
   ```

3. Restart Kafka Connect workers

## Configuration

### Connector Configuration

Create `config/json/source-connector.json`:

```json
{
  "name": "retail-data-simulator",
  "config": {
    "connector.class": "com.retaildatasimulator.kafka.connect.source.RetailDataSimulatorConnector",
    "tasks.max": "1",
    "inventory.topic": "inventory_updates",
    "orders.topic": "customer_orders",
    "logistics.topic": "shipment_updates",
    "poll.interval.ms": "1000",
    "batch.size": "5"
  }
}
```

Deploy the connector:

```bash
curl -X POST -H "Content-Type: application/json" --data @config/json/source-connector.json http://localhost:8083/connectors
```

## Data Schemas

The connector generates data with the following schemas:

**Inventory Events:**

```json
{
  "event_id": "string",
  "event_type": "string",
  "location": "string",
  "sku": "string",
  "quantity": "int",
  "timestamp": "string",
  "reorder_threshold": "int",
  "supplier": "string"
}
```

**Order Events:**

```json
{
  "event_id": "string",
  "event_type": "string",
  "order_id": "string",
  "customer_id": "string",
  "items": [
    {
      "sku": "string",
      "quantity": "int",
      "price": "float",
      "size": "string"
    }
  ],
  "total_amount": "float",
  "payment_method": "string",
  "payment_details": "string",
  "loyalty_points_used": "int",
  "timestamp": "string"
}
```

**Logistics Events:**

```json
{
  "event_id": "string",
  "event_type": "string",
  "shipment_id": "string",
  "carrier": "string",
  "status": "string",
  "estimated_delivery": "string",
  "cost_components": [
    {
      "name": "string",
      "value": "float"
    }
  ],
  "route_optimization_score": "float",
  "carbon_footprint": "float",
  "timestamp": "string"
}
```

## Development

### Customizing Data Generation

To modify the data generation patterns:

1. Edit the generator methods in `RetailDataSimulatorTask.java`:
   - `generateInventoryEvent()`
   - `generateOrderEvent()`
   - `generateLogisticsEvent()`

2. Add new fields to the schemas if needed

### Testing

Run unit tests:

```bash
mvn test
```

For integration testing with a live Kafka cluster:

```bash
docker-compose -f docker-compose-test.yml up -d
mvn verify
```

## Deployment

### Production Deployment Options

1. **Confluent Hub**: Package as a Confluent Hub component
2. **Docker Image**: Build a custom Connect image with the connector
3. **Kubernetes**: Deploy as part of a Kafka Connect cluster on Kubernetes

## Monitoring

Monitor the connector using:

1. Kafka Connect REST API:

   ```bash
   curl http://localhost:8083/connectors/retail-data-simulator/status
   ```

2. JMX metrics (if enabled)

3. Kafka consumer to inspect generated events:

   ```bash
   docker exec -it kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic inventory_updates \
     --from-beginning
   ```

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the Apache License 2.0 - see [LICENSE](LICENSE) for details.
