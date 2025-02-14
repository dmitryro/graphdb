# Experimental Graph Database built in Rust

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![License](https://img.shields.io/badge/license-MIT-blue)
![Version](https://img.shields.io/badge/version-0.0.1--dev-orange)

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Graph Evolution](#graph-evolution)
- [CLI Usage](#cli-usage)
- [Plugin Development](#plugin-development)
- [API Usage](#api-usage)
- [Bolt Protocol](#bolt-protocol)
- [OpenCypher Support](#opencypher-support)

## Overview
This is an experimental graph database built in Rust, supporting multiple query languages and featuring graph evolution tracking.

## Features
- **Query Languages**: Supports Cypher, SQL, and GraphQL.
- **CLI Interface**: Command-line utility for managing and querying graphs.
- **Plugin System**: Drop Rust-based plugins into a designated directory to extend functionality.
- **Graph Evolution**: Track changes in graphs over a specified time window.
- **Bolt Protocol**: Fully supports the Bolt protocol for communication.

## Graph Evolution
Graph evolution allows tracking changes over time using specified time windows. This enables:
- Historical queries
- Change tracking
- Temporal graph analysis

## CLI Usage
The CLI tool allows querying and managing the graph database. 

Example usage:
```sh
./graphdb-cli --query "MATCH (n) RETURN n"
```

To enable plugins:
```sh
./graphdb-cli --enable-plugins
```

## Plugin Development
To create a plugin:
1. Build a Rust-based plugin.
2. Place the compiled `.so` or `.dll` file into the `plugins/` directory.
3. Restart the database to load the plugin.

## API Usage
Run the API server:
```sh
cargo run --release -- serve
```
Modify API endpoints in `src/api.rs`.

## Bolt Protocol
The database fully implements the **Bolt Protocol**, allowing applications to communicate efficiently using binary messaging. This ensures fast and secure transactions with client applications such as Neo4j drivers.

## OpenCypher Support
The database natively supports **OpenCypher**, enabling users to write expressive graph queries similar to Neo4j. Example:
```cypher
MATCH (n:Person)-[r:KNOWS]->(m:Person) RETURN n, r, m;
```
This compatibility allows seamless migration from other OpenCypher-compliant databases.

---

For more details, check the documentation and examples provided.


