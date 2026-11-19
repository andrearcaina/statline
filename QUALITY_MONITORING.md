# Statline Quality and Monitoring Guide

## What Changed

### `src/quality/rules.py`

Adds five Spark validation rules:

- `missing_game_id`
- `invalid_action_number`
- `invalid_time_actual`
- `invalid_period`
- `invalid_shot_result`

### `src/quality/inject_bad_rows.py`

Copies one source event five times and changes each copy so that it fails one
known rule. Each injected row records the rule expected to catch it.

### `src/quality/test_bad_rows.py`

Runs a small test that does not require the real dataset. It verifies that all
five injected rows are caught and that two known-good rows pass.

### `src/quality/main.py`

Validates the real Parquet dataset, writes invalid rows to quarantine, and
creates a JSON summary. The dataset is not cached because the local Spark
container does not have enough heap for the full expanded dataset.

### `Makefile`

Adds these commands:

- `make test-quality`
- `make run-quality`
- `make run-quality-injected`

## Dataset Requirement

Docker does not download the dataset. Confirm that this file exists:

```text
data/cdnnba.parquet
```

If it is missing, download it from the project root:

```sh
curl -L https://huggingface.co/datasets/Vladislav/nba_dataset/resolve/main/cdnnba.parquet \
  -o data/cdnnba.parquet
```

## Start Each Docker Container

Start ClickHouse:

```sh
docker compose up -d clickhouse
```

Start Kafka:

```sh
docker compose up -d kafka
```

Start Spark:

```sh
docker compose up -d spark
```

Confirm that all three containers are running:

```sh
docker compose ps
```

They can also be started together:

```sh
make docker-run
```

If Docker reports an old orphan container, it is unrelated to the quality
test. Remove it with:

```sh
docker compose up -d --remove-orphans
```

## Run the Synthetic Bad-Row Test

This test does not use `data/cdnnba.parquet`:

```sh
make test-quality
```

Expected final message:

```text
PASS: all 5 injected bad rows were caught; both known-good rows passed.
```

## Run Injection Against the Real Dataset

With `data/cdnnba.parquet` present and Spark running:

```sh
make run-quality-injected
```

This reads the real events, appends five labelled bad rows in Spark, validates
all rows, and writes:

```text
data/quality/quality_report.json
data/quality/quarantine.parquet
```

View the report with:

```sh
cat data/quality/quality_report.json
```

The verified results are:

- Total rows processed: **3,337,755**
- Original valid rows: **3,337,750**
- Injected rows: **5**
- Injected rows caught: **5**
- Quarantined rows: **5**
- Original rows flagged: **0**

Run validation without adding test rows with:

```sh
make run-quality
```

## Container Responsibilities

Spark runs the quality rules and injected-row test.

Kafka is started for the complete Statline environment, but the current
quality command does not publish the five injected rows to Kafka.

ClickHouse is also started for the complete environment, but the current
quality command does not insert the injected rows into ClickHouse.

The current evidence therefore proves that the quality stage catches bad rows
before they continue through the pipeline. Testing injected rows end-to-end
through Kafka and ClickHouse would require integrating validation into the
producer and serve components after those components are finalized.

## Stop the Containers

```sh
docker compose down --remove-orphans
```
