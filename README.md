# KVXDB

[![PyPI version](https://img.shields.io/pypi/v/kvxdb.svg)](https://pypi.org/project/kvxdb/)
[![Python versions](https://img.shields.io/pypi/pyversions/kvxdb.svg)](https://pypi.org/project/kvxdb/)
[![Build](https://github.com/your-org/kvxdb/actions/workflows/ci.yml/badge.svg)](https://github.com/your-org/kvxdb/actions)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Coverage Status](https://coveralls.io/repos/github/your-org/kvxdb/badge.svg?branch=main)](https://coveralls.io/github/your-org/kvxdb?branch=main)

---

**KVXDB** is a lightweight, in-memory key–value database inspired by Redis.  
It runs in a separate process or thread, with optional persistence and a simple AI module for vector search and external embeddings integration.

- ⚡ **C core** — minimal, portable, predictable.  
- 🐍 **Python bindings** — clean, modern developer experience.  
- 💾 **Optional persistence** — snapshot storage to disk.  
- 🔍 **Vector search** — simple similarity search over vectors.  
- 🔗 **Embeddings hook** — integration with external ML/AI models.  

---

## Features (MVP)
- In-memory **key–value store** (string keys, string/binary values).  
- **Background process/thread runtime**.  
- **Snapshot persistence** (opt-in).  
- **Vector similarity search**.  
- **Embeddings hook** for AI integration.  
- Installable via **PyPI** (`pip install kvxdb`).  

---

## Quickstart

### Install
```bash
pip install kvxdb
```

### Example
```python
import kvxdb

# Start database in background process
db = kvxdb.start()

# Set & get values
db.set("foo", "bar")
print(db.get("foo"))  # -> "bar"

# Store vectors & search
db.vectors.add("item1", [0.1, 0.2, 0.3])
results = db.vectors.search([0.1, 0.2, 0.29], top_k=3)

# Stop database
db.stop()
```

### Run with Docker
```bash
docker build -t kvxdb .
docker run -p 6379:6379 kvxdb
```

### Run with Docker Compose
```bash
docker compose up
```

---

## Development

### Requirements
- Python **3.10+**
- Linux / macOS (Windows support planned)
- GCC/Clang toolchain

### Setup
```bash
# Install dev dependencies
pip install -r requirements-dev.txt

# Install pre-commit hooks
pip install pre-commit
pre-commit install

# Run linters
ruff check .
black .
```

---

## Roadmap

1. **Stage 1 — Minimal KV engine**
   - C core: basic set/get/delete.
   - Python bindings.
   - Background process management.

2. **Stage 2 — Persistence**
   - Snapshots + configuration.

3. **Stage 3 — Vector search**
   - Store/search vectors.
   - External embeddings hook.

4. **Stage 4 — Packaging & Release**
   - First PyPI release.
   - Docker Hub image.
   - Docs & examples.

---

## License
KVXDB is licensed under the [Apache License 2.0](LICENSE).
