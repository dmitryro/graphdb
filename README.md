# GraphDB
[![Build Status](https://github.com/dmitryro/graphdb/actions/workflows/ci.yaml/badge.svg)](https://github.com/dmitryro/graphdb/actions/workflows/ci.yaml)
[![Rust](https://img.shields.io/badge/Rust-1.72-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/graphdb.svg)](https://crates.io/crates/graphdb)
[![Docs.rs](https://docs.rs/graphdb/badge.svg)](https://docs.rs/graphdb)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)
[![Status](https://img.shields.io/badge/Status-Stable-yellow)](https://github.com/dmitryro/graphdb)

GraphDB is an experimental graph database engine and command-line interface (CLI) optimized for medical and healthcare applications. It empowers developers, researchers, and healthcare professionals to build, query, and analyze interconnected medical data with high context-awareness. By leveraging a graph-native approach, GraphDB unlocks insights from complex relationships that traditional relational databases struggle to handle, making it an ideal complement to existing Electronic Health Record (EHR) systems.

> **Note**: GraphDB is under active development. APIs and behavior may change before the 1.0 release. In production, ensure encryption, authentication, and access controls are configured to meet HIPAA/GDPR compliance requirements.

## 📁 Table of Contents

* [🚑 Why Medical Practices Need GraphDB](#why-medical-practices-need-graphdb)
* [🧠 Key Benefits](#key-benefits)
* [🛠️ What GraphDB Does](#what-graphdb-does)
* [🧹 Quick Example](#quick-example)
* [🏗️ Architecture](#architecture)
* [🔌 How It Works](#how-it-works)
* [🌐 Complementing Existing EHRs](#complementing-existing-ehrs)
* [🧪 Example Use Cases](#example-use-cases)
* [🚀 Getting Started](#getting-started)
  * [📋 Installation Prerequisites](#installation-prerequisites)
  * [🛠️ Building GraphDB](#building-graphdb)
  * [🚀 Running GraphDB Components](#running-graphdb-components)
* [📂 File Structure](#file-structure)
* [📦 Crate/Module Details](#crate/module-details)
* [⚡ Ports, Daemons, and Clusters](#ports-daemons-and-clusters)
* [💻 Command-Line Interface (CLI) Usage](#command-line-interface-cli-usage)
* [🌐 REST API Usage](#rest-api-usage)
* [🗄️ Storage Backends](#storage-backends)
* [🔮 Future Vision: Advanced Querying & AI Integration](#future-vision-advanced-querying--ai-integration)
* [🧬 Medical Ontology Support](#medical-ontology-support)
* [📢 Contributing](#contributing)
* [📜 License](#license)
* [🌐 Links](#links)

## 🚑 Why Medical Practices Need GraphDB

Electronic Health Record (EHR) systems typically rely on linear, table-based relational models. However, medical data is inherently interconnected, forming complex relationships that are challenging to represent or query efficiently in traditional systems. For example:

* Patients have encounters with providers 👩‍⚕️.
* Encounters generate diagnoses, procedures, notes, and billing codes 📝.
* Medications and prescriptions involve drug interactions and side effects 💊.
* Data flows from devices, labs, insurers, pharmacies, and public health databases 📊.

Complex queries, such as:
* "Which patients are at risk based on recent prescriptions and lab results?" ⚠️
* "Which providers might be undercoding based on their encounter history?" 📉
* "Show a patient’s medical, behavioral, and socioeconomic history over the past 3 years." 📈

are inefficient or infeasible in relational models. GraphDB addresses this gap by providing a graph-native database that excels at modeling and querying these relationships, enabling faster, more intuitive insights for healthcare applications.

## 🧠 Key Benefits

GraphDB offers unique advantages for healthcare data management:
* **Intuitive Data Modeling**: Represents complex medical relationships (e.g., patient-provider interactions, drug interactions) as nodes and edges, making data exploration natural and efficient.
* **Powerful Querying**: Supports natural language, Cypher, SQL, and GraphQL queries, enabling both technical and non-technical users to extract insights.
* **Seamless Integration**: Complements existing EHR systems by ingesting data in formats like FHIR, HL7, or CSV, acting as a smart middleware layer.
* **Scalable Architecture**: Supports standalone, daemonized, or clustered deployments for flexibility and performance.
* **Healthcare-Specific Features**: Includes built-in support for medical ontologies (e.g., ICD-10, SNOMED) and planned AI-driven analytics for advanced insights.
* **Open-Source and Extensible**: MIT-licensed with a pluggable architecture, encouraging community contributions and custom extensions.

## 🛠️ What GraphDB Does

GraphDB is designed to handle the complexity of medical data through:
* **Graph-Native Data Model**: Uses vertices (nodes) and edges (relationships) to capture nuanced connections in medical data, such as patient diagnoses or provider interactions.
* **Natural Language Querying**: Transforms high-level or natural language queries into efficient graph query languages (e.g., Cypher, SQL, GraphQL) for ease of use.
* **Flexible Deployment**: Offers a powerful CLI for interactive and scripted use, alongside a daemonized REST API for integration with existing systems.
* **Pluggable Extensions**: Supports healthcare-specific plugins for standards like FHIR, HL7, ICD-10, CPT, and X12.
* **Middleware Capabilities**: Acts as a context-aware layer for legacy or modern EHR systems, enhancing their relational capabilities.
* **Advanced Analytics**: Enables graph analytics, risk modeling, explainable AI, and auditable traceability for compliance and insights.

## 🧹 Quick Example

Here’s a simple Cypher query to find patients diagnosed with Type 2 Diabetes (ICD-10 code E11):
```cypher
MATCH (p:Patient)-[:HAS_DIAGNOSIS]->(d:Diagnosis)
WHERE d.code = "E11"
RETURN p.name, p.age
```

This query traverses the graph to return patient names and ages, demonstrating GraphDB’s ability to handle relational queries efficiently.

## 🏗️ Architecture

GraphDB’s modular, daemonized architecture ensures scalability, performance, and flexibility. Below is a visual representation of its components and their interactions:
```
+-------------------------------------------------------------------------+
|                  graphdb-cli (Interactive & Scriptable Client)          |
| +---------------------------------------------------------------------+ |
| | Parses CLI commands, transforms queries, dispatches to daemons      | |
| +---------------------------------------------------------------------+ |
+------------------------------------|------------------------------------+
                                     | (Local Process / HTTP / gRPC)
                                     ↓
+-------------------------------------------------------------------------+
|                  graphdb-rest_api (REST API Gateway)                    |
| +---------------------------------------------------------------------+ |
| | Exposes RESTful endpoints for programmatic access                  | |
| | Handles authentication, routing, and data serialization             | |
| +---------------------------------------------------------------------+ |
+------------------------------------|------------------------------------+
                                     | (gRPC / Internal IPC)
                                     ↓
+-------------------------------------------------------------------------+
|                  graphdb-daemon (Core Graph Processing Daemon)          |
| +---------------------------------------------------------------------+ |
| | Manages graph state, executes queries, handles concurrency          | |
| | Uses graphdb-lib for graph modeling and query execution            | |
| | Supports single-instance or clustered deployments                  | |
| +---------------------------------------------------------------------+ |
+------------------------------------|------------------------------------+
                                     | (Internal IPC / Storage Protocol)
                                     ↓
+-------------------------------------------------------------------------+
|                  graphdb-storage-daemon (Pluggable Storage Backend)     |
| +---------------------------------------------------------------------+ |
| | Manages persistent storage, indexing, and transactional integrity  | |
| | Supports multiple backends (Postgres, Redis, RocksDB, Sled)         | |
| +---------------------------------------------------------------------+ |
```

This architecture allows independent scaling of components, supporting both lightweight local deployments and distributed, high-performance clusters.

## 🔌 How It Works

GraphDB processes data through a streamlined workflow:
1. **Input Parsing**: Queries from the CLI or REST API (natural language, Cypher, SQL, or GraphQL) are parsed and transformed into an internal graph traversal representation.
2. **Daemonized Execution**: The `graphdb-daemon` handles query execution, maintains graph state, and supports concurrent access. It leverages in-memory caching for performance.
3. **Storage Management**: The `graphdb-storage-daemon` abstracts persistent storage, supporting multiple backends (e.g., Postgres, RocksDB) via pluggable interfaces.
4. **Integration**: GraphDB can operate standalone for graph analysis or integrate into existing healthcare IT pipelines, enhancing data interoperability.

## 🌐 Complementing Existing EHRs

GraphDB enhances, rather than replaces, existing EHR systems by:
* **Ingesting Data**: Supports formats like CSV, HL7, FHIR, or direct Postgres connections, making it easy to import data from EHRs.
* **Transforming Data**: Converts structured and semi-structured data into a queryable graph model, preserving relationships.
* **Enabling Insights**: Facilitates temporal and semantic joins across disparate datasets, uncovering insights hidden in relational structures (e.g., linking patient records with lab results and billing codes).

## 🧪 Example Use Cases

GraphDB’s graph-native approach unlocks powerful healthcare applications:
* **Clinical Decision Support**: Identify drug-allergy interactions or suggest treatment paths by traversing patient history graphs in real-time. 🩺
* **Billing Optimization**: Detect missed CPT coding opportunities or fraudulent billing patterns using graph-based anomaly detection. 💰
* **Patient Risk Modeling**: Build longitudinal graphs of patient medical, behavioral, and socioeconomic factors for predictive analytics and proactive care. 📊
* **Security and Compliance**: Visualize user access logs as graphs to ensure HIPAA/GDPR compliance and detect unauthorized access. 🔒
* **Research and Epidemiology**: Analyze disease propagation networks, identify clinical trial cohorts, or study social determinants of health. 🔬

## 🚀 Getting Started

### 📋 Installation Prerequisites

Before building GraphDB, ensure the following are installed:
* **Rust**: Version 1.72 or higher (`rustup install 1.72`).
* **Cargo**: Included with Rust for building and managing dependencies.
* **Git**: For cloning the repository.
* **Optional Backends** (if used):
  * Postgres: For relational storage.
  * Redis: For caching.
  * RocksDB/Sled: For embedded key-value storage.

### 🛠️ Building GraphDB

1. Clone the repository:
   ```bash
   git clone [https://github.com/dmitryro/graphdb.git](https://github.com/dmitryro/graphdb.git)
   cd graphdb
   ```

2. Build the CLI executable:
   ```bash
   cargo build --workspace --release --bin graphdb-cli
   ```

   The compiled binary will be located at `./target/release/graphdb-cli`.

### 🚀 Running GraphDB Components

GraphDB supports multiple interaction modes:
* **Interactive CLI**: For exploratory querying and management.
* **Scripted CLI**: For automation and batch processing.
* **REST API**: For programmatic integration with other applications.

#### CLI Commands
* **Start Interactive CLI**:
  ```bash
  ./target/release/graphdb-cli --cli
  ```

  Enter commands like `start`, `stop`, `status`, or `rest graph-query`.
* **Start a Single Graph Daemon**:
  ```bash
  ./target/release/graphdb-cli start --port 9001
  ```

  Default port is 8080 if `--port` is omitted.
* **Start a Daemon Cluster**:
  ```bash
  ./target/release/graphdb-cli start --cluster 9001-9003
  ```

  Launches daemons on ports 9001–9003.
* **Start REST API and Storage Daemon**:
  ```bash
  ./target/release/graphdb-cli start --listen-port 8082 --storage-port 8085
  ```

  REST API runs on port 8082, storage daemon on 8085.
* **Stop Components**:
  * Stop all components:
    ```bash
    ./target/release/graphdb-cli stop
    ```

  * Stop specific components:
    ```bash
    ./target/release/graphdb-cli stop rest
    ./target/release/graphdb-cli stop daemon --port 9001
    ./target/release/graphdb-cli stop storage --port 8085
    ```

#### Querying Data
* **Direct CLI Query**:
  ```bash
  ./target/release/graphdb-cli --query "MATCH (n) RETURN n"
  ```

* **Interactive CLI Query**:
  ```
  graphdb-cli> rest graph-query "MATCH (p:Patient) RETURN p.name LIMIT 5"
  ```

* **REST API Query**:
  ```bash
  curl -X POST [http://127.0.0.1:8082/api/v1/query](http://127.0.0.1:8082/api/v1/query) \
    -H "Content-Type: application/json" \
    -d '{"query":"MATCH (n:Person {name: \"Alice\"}) RETURN n"}'
  ```

## 📂 File Structure

The project is organized for modularity and maintainability:
* `graphdb-lib/` 🧠: Core graph engine, data structures, and query parsing.
* `server/` 💻: CLI application (`graphdb-cli`) and its components.
* `daemon-api/` ⚙️: Interfaces for daemon communication (e.g., gRPC).
* `rest-api/` 🌐: RESTful API gateway for external access.
* `storage-daemon-server/` 🗄️: Pluggable storage backend daemon.
* `proto/` 📦: gRPC service definitions for distributed setups.
* `models/medical/` ⚕️: Healthcare-specific graph structures and ontologies.

## 📦 Crate/Module Details

### `graphdb-lib` 🧠
* **Purpose**: Core graph engine with data structures (nodes, edges), traversal algorithms (BFS, DFS, shortest path), and query parsing for Cypher, SQL, and GraphQL.
* **Features**:
  * Efficient in-memory graph representation.
  * Schema management for nodes and relationships.
  * Query execution engine with support for multiple query languages.

### `server` 💻
* **Purpose**: Houses the `graphdb-cli` binary for interactive and scripted use.
* **Subcomponents** (`server/src/cli/`):
  * `cli.rs`: Parses command-line arguments and dispatches commands.
  * `commands.rs`: Defines CLI subcommands using the `clap` crate.
  * `handlers.rs`: Implements logic for commands (e.g., start/stop daemons).
  * `interactive.rs`: Manages the interactive CLI shell.
  * `config.rs`: Handles configuration (ports, data directories) via YAML/TOML.
  * `daemon_management.rs`: Manages daemon lifecycle (spawning, monitoring, stopping).
  * `help_display.rs`: Generates detailed help messages for CLI commands.

### `daemon-api` ⚙️
* **Purpose**: Provides programmatic interfaces for controlling `graphdb-daemon` instances.
* **Features**: Uses gRPC for efficient, language-agnostic communication between components.

### `rest-api` 🌐
* **Purpose**: Exposes RESTful endpoints for programmatic access.
* **Key Endpoints**:
  * `GET /api/v1/health`: Checks system status.
  * `POST /api/v1/query`: Executes graph queries (Cypher, SQL, GraphQL).
  * `POST /api/v1/start/port/{port}`: Starts a single daemon.
  * `POST /api/v1/start/cluster/{start}-{end}`: Starts a daemon cluster.
  * `POST /api/v1/stop`: Shuts down components (optional parameters for specific daemons).
  * `POST /api/v1/ingest` (Planned): Ingests data in formats like FHIR.
  * `GET /api/v1/nodes/{id}` (Planned): Retrieves a specific node.
  * `GET /api/v1/relationships/{id}` (Planned): Retrieves a specific relationship.

### `storage-daemon-server` 🗄️
* **Purpose**: Manages persistent storage with a pluggable architecture.
* **Supported Backends**: Postgres, Redis, RocksDB, Sled.
* **Features**: Ensures data durability, indexing, and transactional integrity.

### `proto` 📦
* **Purpose**: Defines gRPC Protobuf messages and services for distributed communication.

### `models/medical` ⚕️
* **Purpose**: Provides healthcare-specific graph structures and ontologies for context-aware queries.

## ⚡ Ports, Daemons, and Clusters

GraphDB components run as independent daemons, communicating via defined ports:

| Component             | Default Port | Description                              |
|-----------------------|--------------|------------------------------------------|
| `graphdb-daemon`      | 8080         | Core graph processing daemon            |
| `graphdb-rest_api`    | 8082         | REST API gateway                        |
| `graphdb-storage-daemon` | 8085      | Persistent storage daemon                |

* **Single Instance**: Suitable for local development or small-scale deployments.
* **Cluster Mode**: Supports distributed processing across multiple ports (e.g., 9001–9003) for scalability.
* **Use Cases**:
  * Interactive querying: CLI.
  * Automation/scripting: REST API.
  * Batch ingestion: CLI + Daemon.
  * Distributed processing: gRPC (planned).

## 💻 Command-Line Interface (CLI) Usage

The `graphdb-cli` binary provides flexible interaction options:
```bash
./target/release/graphdb-cli --cli                        # Start interactive shell
./target/release/graphdb-cli start --port 9001           # Start single daemon
./target/release/graphdb-cli start --cluster 9001-9003   # Start daemon cluster
./target/release/graphdb-cli stop                        # Stop all components
./target/release/graphdb-cli view-graph --graph-id 42    # View graph by ID
./target/release/graphdb-cli --query "MATCH (n) RETURN n" # Execute direct query
```

## 🌐 REST API Usage

Interact with GraphDB programmatically via the REST API:
```bash
# Check system health
curl [http://127.0.0.1:8082/api/v1/health](http://127.0.0.1:8082/api/v1/health)

# Execute a graph query
curl -X POST [http://127.0.0.1:8082/api/v1/query](http://127.0.0.1:8082/api/v1/query) \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n:Person {name: \"Alice\"}) RETURN n"}'
```

## 🗄️ Storage Backends

GraphDB supports pluggable storage backends:
* **Postgres**: Relational persistence and SQL queries.
* **Redis**: High-speed caching for transient data.
* **RocksDB**: Embedded key-value store for local performance.
* **Sled**: Lock-free, embedded database for Rust.
Custom backends can be implemented via trait interfaces.

## 🔮 Future Vision: Advanced Querying & AI Integration

GraphDB aims to evolve into a more intelligent platform:
* **Natural Language Processing (NLP)**: Enhanced support for conversational queries, enabling non-technical users to interact with the database.
* **AI-Driven Insights**: Integration with machine learning models for predictive analytics, such as identifying at-risk patients or optimizing clinical workflows.
* **Graph Visualization**: A planned UI for exploring and visualizing graph data interactively.
* **Distributed gRPC**: Enhanced support for multi-language, distributed deployments.

## 🧬 Medical Ontology Support

GraphDB supports key healthcare standards:
* FHIR (STU3/STU4)
* HL7 (v2/v3)
* CPT, ICD-10, LOINC, SNOMED
* X12 (837/835 claims)
* **Planned**: Retrieval-Augmented Generation (RAG) for NLP queries, time-series support for EEG/EKG data.

## 📢 Contributing

We welcome contributions to enhance GraphDB:
* ✅ Cypher query support (complete)
* [ ] NLP pipeline integration
* [ ] gRPC enhancements
* [ ] Graph explorer UI

Submit pull requests or report issues at [https://github.com/dmitryro/graphdb/issues](https://github.com/dmitryro/graphdb/issues).

## 📜 License

MIT License (see [LICENSE](./LICENSE)).

## 🌐 Links

* **GitHub**: [https://github.com/dmitryro/graphdb](https://github.com/dmitryro/graphdb)
* **Issues**: [https://github.com/dmitryro/graphdb/issues](https://github.com/dmitryro/graphdb/issues)
* **Documentation**: [https://docs.rs/graphdb](https://docs.rs/graphdb)
* **Crates.io**: [https://crates.io/crates/graphdb](https://crates.io/crates/graphdb)

