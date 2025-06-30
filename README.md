# GraphDB
[![Build Status](https://github.com/dmitryro/graphdb/actions/workflows/ci.yaml/badge.svg)](https://github.com/dmitryro/graphdb/actions/workflows/ci.yaml)
[![Rust](https://img.shields.io/badge/Rust-1.72-orange?logo=rust&logoColor=white)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/graphdb.svg)](https://crates.io/crates/graphdb)
[![Docs.rs](https://docs.rs/graphdb/badge.svg)](https://docs.rs/graphdb)
[![License](https://img.shields.io/badge/License-MIT-green)](./LICENSE)
[![Status](https://img.shields.io/badge/Status-Stable-yellow)](https://github.com/dmitryro/graphdb)

GraphDB is an experimental graph database engine and command-line interface (CLI) optimized for medical and healthcare applications. It allows developers, researchers, and integrators to build, query, and reason about interconnected medical data with high context-awareness.

---

## 📁 Table of Contents

* [🚑 Why Medical Practices Need GraphDB](#-why-medical-practices-need-graphdb)
* [🧠 What GraphDB Does](#-what-graphdb-does)
* [🧹 Quick Example](#-quick-example)
* [🧹 Architecture](#-architecture)
* [🛠️ How It Works](#%ef%b8%8f-how-it-works)
* [🔌 Complementing Existing EHRs](#-complementing-existing-ehrs)
* [🧪 Example Use Cases](#-example-use-cases)
* [🚀 Getting Started](#-getting-started)
* [📂 File Structure](#-file-structure)
* [📦 Crate/Module Details](#-cratemodule-details)
* [⚡ Ports, Daemons, Clusters, and API Gateway](#-ports-daemons-clusters-and-api-gateway)
* [💻 Command-Line Usage](#-command-line-usage)
* [🌐 REST API Usage](#-rest-api-usage)
* [🗄️ Storage Backends](#-storage-backends)
* [🧬 Medical Ontology Support](#-medical-ontology-support)
* [📢 Contributing](#-contributing)
* [📜 License](#-license)
* [🌐 Links](#-links)

---

## 🚑 Why Medical Practices Need GraphDB

Electronic Health Record (EHR) systems are built around linear, table-based relational models. However, the real world of medicine is graph-like:

* Patients have **encounters** with **providers**
* Encounters yield **diagnoses**, **procedures**, **notes**, and **billing codes**
* Medications and **prescriptions** have drug **interactions** and **side effects**
* Data flows from **devices**, **labs**, **insurers**, **pharmacies**, and **public health databases**

Traditional EHR systems cannot easily represent or traverse these relationships. Complex queries like:

* "Which patients are at risk given their recent prescriptions and lab results?"
* "Which providers are likely to undercode given their encounter history?"
* "Show patient’s medical, behavioral, and socioeconomic graph over the past 3 years."

...are inefficient or infeasible using standard relational models.

GraphDB fills this gap.

---

## 🧠 What GraphDB Does

* Provides a **graph-native data model** with vertices and edges to capture rich relationships
* Converts **natural or high-level language queries** into **Cypher** or similar graph queries
* Offers both **CLI** and **daemonized API** modes for flexible integration
* Supports **plugin-based extensions** for healthcare domains (FHIR, HL7, ICD-10, CPT, X12, etc.)
* Can function as a smart middleware layer for legacy or modern EHRs
* Enables **graph analytics**, **risk modeling**, **explainable AI**, and **auditable traceability**

---

## 🧹 Quick Example

```cypher
MATCH (p:Patient)-[:HAS_DIAGNOSIS]->(d:Diagnosis)
WHERE d.code = "E11"
RETURN p.name, p.age
```

---

## 🧹 Architecture

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

---

## 🛠️ How It Works

* Parses CLI or REST input to generate internal graph traversal queries
* Runs as a daemon (or cluster of daemons) that maintains state and handles concurrent access
* Offers shared-memory model and optional in-memory caching for high performance
* Can be used as a standalone tool or embedded in analytics pipelines

---

## 🔌 Complementing Existing EHRs

GraphDB complements, not replaces, existing systems:

* Works with exported EHR data (CSV, HL7, FHIR, Postgres)
* Ingests and transforms structured data into a queryable graph
* Offers temporal and semantic joins across otherwise disjoint datasets

---

## 🧪 Example Use Cases

* **Clinical Decision Support**: Detect drug-allergy interactions in real time
* **Billing Optimization**: Uncover missed CPT coding opportunities via graph correlation
* **Patient Risk Modeling**: Generate longitudinal graphs for predictive analytics
* **Security Auditing**: Visualize user access/edit logs for HIPAA compliance

---

## 🚀 Getting Started

```bash
cargo build --workspace --release --bin graphdb-cli
./target/release/graphdb-cli --cli
./target/release/graphdb-cli --help
./target/release/graphdb-cli start --port 9001
./target/release/graphdb-cli start --cluster 9001-9003
./target/release/graphdb-cli --query "MATCH (n) RETURN n"
```

---

## 📂 File Structure

* `graphdb-lib/` — Core engine and graph models
* `graphdb-cli/` — Interactive and batch CLI
* `graphdb-daemon/` — Daemon process (gRPC/REST server)
* `graphdb-rest_api/` — REST API gateway
* `proto/` — gRPC definitions
* `models/medical/` — Domain-specific graph structures

---

## 📦 Crate/Module Details

### `graphdb-daemon`

Handles background operations, daemon lifecycle, graph state management, and request serving.

### `graphdb-rest_api`

RESTful interface for programmatic access:

* `/api/v1/health`
* `/api/v1/query`
* `/api/v1/start/port/{port}`
* `/api/v1/start/cluster/{start}-{end}`
* `/api/v1/stop`

### `graphdb-lib`

Graph models, traversal algorithms, query parsing engine.

### `graphdb-server` (`cli.rs`)

Command-line executable that starts and interacts with the daemon and API.

### `proto`

Defines gRPC interfaces for future distributed/multi-language setups.

---

## ⚡ Ports, Daemons, Clusters, and API Gateway

| Use Case                      | Interface      |
| ----------------------------- | -------------- |
| Interactive query exploration | CLI            |
| Automation & scripting        | REST API       |
| Batch ingestion               | CLI + Daemon   |
| Distributed processing        | gRPC (planned) |

---

## 💻 Command-Line Usage

```bash
./graphdb-cli --cli
./graphdb-cli start --port 9001
./graphdb-cli start --cluster 9001-9003
./graphdb-cli stop
./graphdb-cli view-graph --graph-id 42
./graphdb-cli --query "MATCH (n) RETURN n"
```

---

## 🌐 REST API Usage

```bash
curl http://127.0.0.1:8082/api/v1/health
curl -X POST http://127.0.0.1:8082/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query":"MATCH (n:Person {name: \"Alice\"}) RETURN n"}'
```

---

## 🗄️ Storage Backends

Supported:

* Postgres
* Redis
* RocksDB
* Sled

Pluggable via trait interfaces for custom backends.

---

## 🧬 Medical Ontology Support

* FHIR STU3/STU4
* HL7 v2/v3
* CPT / ICD / LOINC / SNOMED
* X12 (837/835 claims)
* Future: RAG/NLP plugins, time series (EEG/EKG)

---

## 📢 Contributing

* [x] Cypher query support
* [ ] NLP pipeline integration
* [ ] gRPC enhancements
* [ ] Graph explorer UI

---

## 📜 License

MIT License

---

## 🌐 Links

* GitHub: [https://github.com/dmitryro/graphdb](https://github.com/dmitryro/graphdb)
* Issues: [https://github.com/dmitryro/graphdb/issues](https://github.com/dmitryro/graphdb/issues)

---

> ⚠️ **Note on Security & Compliance:** In production, configure encryption, authentication, and access control to meet HIPAA/GDPR requirements.

> ✨ **Note:** This is an experimental system under active development. APIs and behavior may change before 1.0 release.

