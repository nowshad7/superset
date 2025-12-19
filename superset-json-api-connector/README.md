# Superset JSON API Connector

A SQLAlchemy dialect for connecting Apache Superset to JSON REST APIs.

## Features

- Connect Superset to any JSON API endpoint
- Automatic schema inference
- Support for authentication (API keys, Bearer tokens)
- SQL-like querying with WHERE and LIMIT clauses
- Handles various JSON response formats

## Installation

### In Superset Docker Container:

```bash
# Copy connector to container
docker cp ./superset-json-api-connector superset:/tmp/json-api-connector

# Install
docker exec superset pip install -e /tmp/json-api-connector

# Restart Superset
docker restart superset