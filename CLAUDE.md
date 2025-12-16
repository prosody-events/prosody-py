# CLAUDE.md

## Development Setup

This is a Python/Rust hybrid project using PyO3 and Maturin.

### Prerequisites
- Docker Compose running with Kafka and Cassandra services
- Python 3.8+ with a virtual environment
- Rust toolchain

### Running Tests

**IMPORTANT**: Always rebuild before running tests:

```bash
# Activate virtual environment
source .venv/bin/activate

# Build and install the package (required before running tests)
maturin develop --extras dev

# Run tests and save output to a file for reference
pytest -v 2>&1 | tee /tmp/pytest-output.txt
```

The `maturin develop` step compiles the Rust code and installs the package in development mode. Skipping this step will run tests against stale code.

**Save test output**: Always redirect test output to a file (e.g., using `tee` or `>`) so you can refer back to it without re-running tests. Avoid using `head`, `tail`, or `grep` on test output streams as this often leads to needing to re-run tests multiple times.

### Local Services

Start required services with:
```bash
docker-compose up -d
```

Services:
- Kafka: localhost:9094
- Cassandra: localhost:9042
- Grafana (OTEL): localhost:3000

### Thread Safety

Tests use `tsasync.Event` and `tsasync.Channel` instead of `asyncio.Event` and `asyncio.Queue` because handlers are called from Rust threads, not the Python event loop thread. Standard asyncio primitives are not thread-safe for cross-thread signaling.
