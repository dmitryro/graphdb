# GraphDB

[![Rust](https://img.shields.io/badge/Rust-1.72-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)
[![Status](https://img.shields.io/badge/Status-Stable-yellow)](https://github.com/dmitryro/graphdb)

GraphDB is an experimental graph database engine and command-line interface (CLI) optimized for medical and healthcare applications. It allows developers, researchers, and integrators to build, query, and reason about interconnected medical data with high context-awareness.

---

## 📁 Table of Contents

- [🚑 Why Medical Practices Need GraphDB](#-why-medical-practices-need-graphdb)
- [🧠 What GraphDB Does](#-what-graphdb-does)
- [🧩 Architecture](#-architecture)
- [🛠️ How It Works](#️-how-it-works)
- [🔌 Complementing Existing EHRs](#-complementing-existing-ehrs)
- [🧪 Example Use Cases](#-example-use-cases)
- [🚀 Getting Started](#-getting-started)
- [📂 File Structure](#-file-structure)
- [📦 Crate/Module Details](#-cratemodule-details)
  - [graphdb-daemon](#graphdb-daemon)
  - [graphdb-rest_api](#graphdb-rest_api)
  - [graphdb-lib](#graphdb-lib)
  - [graphdb-server (CLI)](#graphdb-server-cli)
  - [proto (gRPC)](#proto-grpc)
- [⚡ Ports, Daemons, Clusters, and API Gateway](#-ports-daemons-clusters-and-api-gateway)
- [🖥️ Command-Line Usage](#️-command-line-usage)
- [🌐 REST API Usage](#-rest-api-usage)
- [🗄️ Storage Backends](#-storage-backends)
- [🧬 Medical Ontology Support](#-medical-ontology-support)
- [📢 Contributing](#-contributing)
- [📜 License](#-license)
- [🌐 Links](#-links)

---

## 🚑 Why Medical Practices Need GraphDB

Electronic Health Record (EHR) systems are built around linear, table-based relational models. However, the real world of medicine is graph-like:
- Patients have **encounters** with **providers**
- Encounters yield **diagnoses**, **procedures**, **notes**, and **billing codes**
- Medications and **prescriptions** have drug **interactions** and **side effects**
- Data flows from **devices**, **labs**, **insurers**, **pharmacies**, and **public health databases**

Traditional EHR systems cannot easily represent or traverse these relationships in a meaningful way. Queries like:
- "Which patients are at risk given their recent prescriptions and lab results?"
- "Which providers are likely to undercode given their encounter history?"
- "Show patient’s medical, behavioral, and socioeconomic graph over the past 3 years."

...are difficult or impossible to perform efficiently using traditional relational models.

GraphDB fills this gap.

---

## 🧠 What GraphDB Does

- Provides a **graph-native data model** with vertices and edges to capture rich relationships
- Converts **natural or high-level language queries** into **Cypher** or similar graph queries
- Offers both **CLI** and **daemonized API** modes for flexible integration
- Supports **plugin-based extensions** for different healthcare domains or data standards (FHIR, HL7, ICD-10, CPT, X12, etc.)
- Can function as an intelligent middleware layer in front of legacy EHRs or in new applications
- Enables **graph analytics**, **risk modeling**, **explainable AI**, and **audit trails**

---

## 🧩 Architecture

```
graphdb-cli  (interactive + scriptable)
    |
    ├── Parses CLI input and transforms queries
    ├── Talks to daemonized server via gRPC / REST
    ↓
graphdb-daemon (daemon)
    ├── Accepts and serves incoming requests
    ├── Performs language transformation → Cypher
    ├── Uses graphdb-lib to model/query graph
    └── Stores into modular backends (Postgres, Redis, RocksDB, Sled)
```

- **Medical domain layer**: Models entities like patients, doctors, labs, meds, visits, diagnoses
- **Query parser**: Supports Cypher, GraphQL, SQL, and future high-level "contextual" query DSL
- **Shared memory + IPC**: Ensures inter-process communication for stateful operations

---

## 🛠️ How It Works

- **Language transformation**: Free-text queries (or CLI args) are parsed and mapped to internal graph traversals
- **Daemon mode**: The daemon can run standalone, process API/gRPC requests from your EHR or analytics system
- **Shared memory model**: Ensures fast lookup and IPC, storing daemon state and KV pairs for efficient command execution
- **CLI**: Launch `graphdb-cli --cli` to explore and prototype queries interactively

---

## 🔌 Complementing Existing EHRs

GraphDB is not a replacement—but a powerful **overlay** or **extension layer** for:
- Legacy EHR platforms (e.g., Epic, Cerner) that export data into structured formats
- Modern FHIR APIs that expose resources but lack real graph traversals
- Custom healthcare analytics or NLP pipelines needing contextual joins

It enables **low-friction augmentation**:
- Load data from Postgres, CSV, or HL7 feed
- Convert to graph structure with Python, C#, or Rust ingestion tools
- Query across time, space, events, and categories

---

## 🧪 Example Use Cases

- **Clinical Decision Support**: Show possible interactions between prescribed meds and patient’s allergy + diagnosis history
- **Revenue Optimization**: Correlate CPT codes with diagnosis patterns for missed billing opportunities
- **Patient Risk Graphs**: Build a temporal graph of lifestyle, clinical, and claim data to assess readmission risk
- **Auditing**: Graph of users, edits, access logs across time for HIPAA compliance

---

## 🚀 Getting Started

```bash
# Build the entire workspace for a production binary
cargo build --workspace --release --bin graphdb-cli

# Launch interactive CLI
./target/release/graphdb-cli --cli

# View available commands
./target/release/graphdb-cli --help

# Start daemon on a port (e.g., 9001)
./target/release/graphdb-cli start --port 9001

# Start a cluster of daemons (e.g., ports 9001-9003)
./target/release/graphdb-cli start --cluster 9001-9003

# Execute a query directly
./target/release/graphdb-cli --query "MATCH (n) RETURN n"
```

**Dependencies:**  
- Rust (1.72+)
- Recommended: Use [rustup](https://rustup.rs/) to manage toolchains  
- Optional for storage: Postgres, Redis, RocksDB, Sled (see [Storage Backends](#-storage-backends))

---

## 📂 File Structure

- `graphdb-lib/` — Core library: data models, parsers, engines
- `graphdb-cli/` — CLI frontend
- `graphdb-daemon/` — Server backend (daemon)
- `graphdb-rest_api/` — RESTful API server (API gateway)
- `proto/` — gRPC definitions and Rust bindings (planned for advanced inter-service communication)
- `models/medical/` — Vertices/edges for domain modeling

---

## 📦 Crate/Module Details

### `graphdb-daemon`
- **What it is:** The daemon process that runs as a background service. Handles incoming requests (from CLI or REST API), manages graph state, and orchestrates data storage.
- **Key features:** Can be started/stopped independently; supports binding to specific ports; manages graph operations and IPC.
- **How to use:** Launched automatically by the CLI or manually via CLI commands.

### `graphdb-rest_api`
- **What it is:** The RESTful API server for remote or programmatic access to graph operations. Serves as an API gateway.
- **Endpoints include:**
  - `/api/v1/health` — Health check endpoint.
  - `/api/v1/query` — Accepts Cypher/SQL/GraphQL queries (see usage below).
  - `/api/v1/start/port/{port}` — Start a daemon on a specific port.
  - `/api/v1/start/cluster/{start}-{end}` — Start cluster of daemons on port range.
  - `/api/v1/stop` — Stop all daemons and REST API server.
- **How to use:** Start via CLI, then use `curl` or HTTP client to access endpoints.

### `graphdb-lib`
- **What it is:** Core library for graph models, algorithms, and the query parser.
- **Includes:**
  - Query parsing logic (`query_parser`): Detects and classifies query types (Cypher, SQL, GraphQL).
  - Data structures and algorithms for graph storage, traversal, and analysis.
- **How to use:** Used internally by CLI, daemon, and REST API.

### `graphdb-server` (`cli.rs`)
- **What it is:** The main executable and CLI frontend for GraphDB. Handles user input, command parsing, and calls into the daemon or REST API.
- **How it works:** Supports interactive and batch command-line use; can start/stop daemons, execute queries, and manage graphs.

### `proto` (gRPC)
- **What it is:** Contains `.proto` files and Rust bindings to enable gRPC-based inter-service communication (planned).
- **Structure:**
  ```
  proto
  ├── Cargo.toml
  ├── build.rs
  ├── graphdb.proto
  └── src
      ├── client.rs
      ├── lib.rs
      └── server.rs
  ```
- **How to use:** `graphdb.proto` defines the gRPC service and messages. After changes, run `cargo build --workspace --release` or use `build.rs` for codegen.
- **Use case:** Will allow distributed GraphDB clusters and advanced integrations with other microservices and languages (e.g., Go, Python).

---

## ⚡ Ports, Daemons, Clusters, and API Gateway

### **TCP Ports and Daemon Management**
- **Single Daemon:**  
  Start a daemon on a specific TCP port (e.g., `9001`) to handle graph operations:
  ```bash
  ./graphdb-cli start --port 9001
  ```
  The daemon will bind to that port and serve requests.

- **Cluster Mode:**  
  Start a cluster of daemons on a contiguous port range:
  ```bash
  ./graphdb-cli start --cluster 9001-9003
  ```
  Each daemon instance binds to one port in the range; ideal for scaling, sharding, or high-availability setups.

- **API Gateway:**  
  The REST API server (API gateway) listens on a specified port (default: `8082`). All RESTful endpoints (query, health, daemon management) are served from this port.

  Example:
  ```bash
  ./graphdb-cli start --listen-port 8082
  # or programmatically via the REST API endpoints (see below)
  ```

### **Planned gRPC API**
- While REST endpoints are available now, a gRPC service is planned and defined in the `proto/` directory. This will enable high-performance, strongly-typed inter-service communication and support for multi-language clients and distributed topologies.

---

## 🖥️ Command-Line Usage

GraphDB provides a powerful CLI with multiple modes and commands.

### Launch Interactive CLI

```bash
./graphdb-cli --cli
```
- Enter queries interactively (Cypher, SQL, GraphQL, or contextual DSL).

### Command-Line Flags

- `--cli` : Launch in interactive console mode.
- `--enable-plugins` : Enable experimental plugin support.
- `--query <QUERY>` : Execute a query directly from the command line.
- `--help` : Show all available commands and options.
- `--listen-port <PORT>`: Specify the port for the REST API server (default: 8082).

### Subcommands

- `start` : Start the graphdb daemon (optionally as a cluster, supports port options).
- `stop` : Stop all running daemons.
- `view-graph --graph-id <ID>` : View a specific graph.
- `view-graph-history --graph-id <ID> [--start-date <date>] [--end-date <date>]` : View historical graph data.
- `index-node --node-id <ID>` : Index a specific node.
- `cache-node-state --node-id <ID>` : Cache state for a node.

**Example Usage:**

```bash
./graphdb-cli start --port 9001
./graphdb-cli start --cluster 9001-9003
./graphdb-cli stop
./graphdb-cli view-graph --graph-id 42
./graphdb-cli --query "MATCH (n) RETURN n"
```

---

## 🌐 REST API Usage

Once the REST API server is running (default port: 8082), you can interact with it using `curl` or any HTTP client.

### Health Check

```bash
curl http://127.0.0.1:8082/api/v1/health
```

### Query Endpoint

Send Cypher/SQL/GraphQL queries:

```bash
curl -X POST http://127.0.0.1:8082/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n:Person {name: \"Alice\"}) RETURN n"}'
```

**Response Example:**
```json
{"message":"Cypher query detected: MATCH (n:Person {name: \"Alice\"}) RETURN n","status":"success"}
```

### Start/Stop Daemon (RESTful)

Start daemon on a specific port:

```bash
curl -X POST http://127.0.0.1:8082/api/v1/start/port/9001
```

Start cluster of daemons on port range:

```bash
curl -X POST http://127.0.0.1:8082/api/v1/start/cluster/9001-9003
```

Stop all daemons and the REST API server:

```bash
curl -X POST http://127.0.0.1:8082/api/v1/stop
```

---

## 🗄️ Storage Backends

GraphDB is designed to be modular and backend-agnostic. It can use multiple storage engines for persistence and fast access:

- **Postgres**: For relational storage and SQL-based analytics.
- **Redis**: For in-memory, high-speed caching and ephemeral data.
- **RocksDB**: For high-performance, embedded key-value storage.
- **Sled**: For lightweight, pure-Rust key-value storage.
- Future support for custom adapters and distributed backends.

You can select or configure backends via daemon/server configuration or by implementing the required trait interfaces.

---

## 🧬 Medical Ontology Support

- FHIR/STU3/STU4 resources
- HL7v2 / HL7v3 messages
- CPT/ICD/LOINC/SNOMED mappings
- Claims (837P/837I), EOBs (835), X12
- Future plugins: NLP, RAG pipelines, EKG/EEG time series

---

## 📢 Contributing

- [x] Support Cypher queries
- [ ] Add more medical vocabularies
- [ ] NLP for note parsing
- [ ] gRPC API enhancements
- [ ] Advanced CLI search/filter tools

PRs welcome!

---

## 📜 License

MIT License

---

## 🌐 Links

- GitHub: [https://github.com/dmitryro/graphdb](https://github.com/dmitryro/graphdb)
- Issues: [https://github.com/dmitryro/graphdb/issues](https://github.com/dmitryro/graphdb/issues)
