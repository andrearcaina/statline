.PHONY: run tidy download build validate fmt fmt-check lint fix check clean \
	docker-build docker-run docker-reset docker-stop docker-clean

run:
	uv run uvicorn src.main:app --reload --no-access-log --log-level warning

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

check: tidy download fmt fix lint validate

clean:
	rm -rf .venv .pytest_cache .ruff_cache __pycache__ src/__pycache__ src/**/__pycache__

docker-build:
	docker compose build

docker-run:
	docker compose up -d

docker-reset: docker-clean docker-run

docker-stop:
	docker compose down --remove-orphans

docker-clean:
	docker compose down --volumes --remove-orphans
