# Install Rust and necessary tools
bootstrap:
	curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
	cargo install maturin cargo-udeps taplo-cli bacon

# Start Kafka and related services using Docker Compose
up:
	docker-compose up -d

# Update project dependencies
update:
	cargo update

# Open the Kafka console in a web browser
console:
	open "http://localhost:8080/topics"

# Format Rust code and TOML files
format:
	cargo fmt
	taplo fmt

# Build the project
build:
	maturin develop --extras dev

build-test:
	maturin develop --extras dev --features admin-client

# Check for compilation errors without building
check:
	cargo check

# Watch for changes and check for compilation errors
check-watch:
	bacon

# Run Clippy for linting
lint:
	cargo clippy

# Watch for changes and run Clippy
lint-watch:
	bacon --job clippy

# Run tests (starts Kafka services first)
test: up build-test
	pytest

# Watch for changes and run tests
test-watch: up build-test
	ptw

# Run tracing integration test with OpenTelemetry collector
test-tracing:
	OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
	OTEL_SERVICE_NAME=prosody-python-tracing-test \
	pytest -m "" tests/test_tracing_integration.py::test_complete_distributed_trace -v -s

shell: up build
	python -m asyncio

# Check for unused dependencies
# note: requires installing nightly with `rustup install nightly`
dependencies:
	cargo +nightly udeps

# Stop and remove Docker containers and volumes
reset:
	docker-compose down --volumes
