# Kafka - Schema registry with Avro

A sample project producing and consuming records using a Confluent Schema Registry and Avro.

## Goals

- [x] Set up Kafka, Kafka Dashboard and a Schema Registry;
- [x] Define schemas;
- [x] Produce records based on schemas;
- [x] Consume records based on schemas;
- [x] Evolve a schema;

## Running locally

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Docker Compose;
- Go 1.26+;
- `make`, `jq` and `curl`;
- `reflex` (optional — only needed for hot reload with `make producer` / `make consumer`).

### 1. Install code-generation tooling

```bash
make install
```

Installs `gogen-avro`, used to generate Go types from the Avro schemas.

### 2. Start the infrastructure

```bash
docker compose up -d
```

Starts a 3-broker Kafka cluster (KRaft with a dedicated controller), a Confluent Schema Registry, the Schema Registry UI, a Redpanda Console dashboard, and a Postgres + Debezium Connect stack.

### 3. Create the topic

```bash
make setup-kafka
```

Creates `work.firmino.orders` (3 partitions). Topics are not auto-created in this cluster, so this step is required.

### 4. Register the schemas

```bash
make apply-schemas
```

Registers every schema in `schemas/` with the registry, using the file name as the subject. The producer does not auto-register schemas, so this step is required before producing.

### 5. Generate Go types from the schemas

Only needed after a schema changes:

```bash
make gen-schemas
```

### 6. Run the producer and consumer

In two separate terminals:

```bash
make producer   # or: go run . producer
make consumer   # or: go run . consumer
```

`make producer` / `make consumer` reload on every `.go` change (requires `reflex`: `go install github.com/cespare/reflex@latest`); plain `go run . <command>` works too.

The producer builds a random `OrderPlaced` event, serializes it with Avro against the registered schema, and writes it to `work.firmino.orders`. The consumer reads the topic, deserializes against the registry, and prints each event.

### Useful endpoints

| Service | URL |
| --- | --- |
| Redpanda Console (Kafka dashboard) | http://localhost:8090 |
| Schema Registry UI | http://localhost:8002 |
| Schema Registry REST API | http://localhost:8081 |
| Debezium Connect | http://localhost:8083 |

### Tear down

```bash
docker compose down       # keep data
docker compose down -v    # also delete the volumes
```

## Out of scope

- Clean arch, hexagonal arch;
- Production-ready setup;