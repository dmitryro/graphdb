
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
# Build and launch
cargo build --release --bin graphdb-cli
./graphdb-cli --cli

# View available commands
./graphdb-cli --help

# Start daemon
./graphdb-cli start

# Execute contextual query
./graphdb-cli view-graph --graph-id 42
```

---

## 📂 File Structure

- `graphdb-lib/` — Core library: data models, parsers, engines
- `graphdb-cli/` — CLI frontend
- `graphdb-daemon/` — Server backend
- `models/medical/` — Vertices/edges for domain modeling

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

