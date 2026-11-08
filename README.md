# Statline

An NBA play-by-play data pipeline

## Running locally

Install [GNU Make](https://www.gnu.org/software/make/), [`uv`](https://docs.astral.sh/uv/), and [Docker](https://www.docker.com/get-started/).

Linux-based operating systems are highly recommended for ease of development and GNU Make accessibility. If you're on Windows, it is advised to use WSL2 with Docker Desktop.

### Run everything

This one command performs the project setup and validation, starts the Docker services, and runs the entire pipeline:

```sh
make run
```

### Run each part separately

```sh
# Install dependencies, formatting, lint checks, and validation
make check

# Start Spark, ClickHouse, and Kafka services
make docker-run

# Run each pipeline entrypoint individually
make run-batch
make run-producer
make run-consumer

# Run the entire batch and streaming pipeline after Docker is running
make run-pipeline
```
