.PHONY: permissions run prepare-data run-batch run-deep-part run-producer run-consumer run-pipeline run-serve query-serve stream-results \
	tidy download build validate fmt fmt-check lint fix check clean \
	docker-build docker-run docker-reset docker-stop docker-clean

permissions:
	chmod 777 data

# run EVERYTHING with one command
run: check docker-run run-pipeline

prepare-data: permissions
	docker compose exec -T spark /opt/spark/bin/spark-submit src/batch/prepare.py

run-batch: prepare-data
	docker compose exec -T spark /opt/spark/bin/spark-submit src/batch/main.py

run-deep-part:
	docker compose exec -T spark /opt/spark/bin/spark-submit src/batch/deep_part.py --prepare-csv --prepare-partitioned

run-producer: prepare-data
	uv run python -m src.stream.producer

run-consumer:
	uv run python -m src.stream.consumer

run-serve:
	uv run python -m src.serve.main load
	uv run python -m src.serve.main query --output data/serve_measurements.json

query-serve:
	uv run python -m src.serve.main query --output data/serve_measurements.json

stream-results:
	uv run python -m src.serve.main stream

run-pipeline:
	$(MAKE) run-batch
	$(MAKE) run-serve
	$(MAKE) run-producer
	$(MAKE) run-consumer
	$(MAKE) stream-results

tidy:
	uv lock

download:
	uv sync --locked

fmt:
	uv run ruff format src

fix:
	uv run ruff check --fix src

lint:
	uv run ruff check src

validate:
	uv run python -m compileall -q src

check: permissions tidy download fmt fix lint validate

clean:
	rm -rf .venv .pytest_cache .ruff_cache __pycache__ src/__pycache__ src/**/__pycache__

docker-build:
	docker compose build

docker-run:
	docker compose up -d --wait --wait-timeout 120

docker-reset: docker-clean docker-run

docker-stop:
	docker compose down --remove-orphans

docker-clean:
	docker compose down --volumes --remove-orphans
