# Statline

An NBA play-by-play data pipeline

## Running locally

Install [GNU Make](https://www.gnu.org/software/make/), [`uv`](https://docs.astral.sh/uv/), and [Docker](https://www.docker.com/get-started/).

Linux-based operating systems are HIGHLY recommended for ease of development and native GNU Make support.

If you're using Windows, we recommend WSL2 with Docker Desktop for a Linux-like development environment. 
Alternatively, if you prefer working directly in Windows, you can install GNU Make using a package manager such as Chocolatey or Scoop:

```sh
# install using chocolately
> choco install make

# or scoop
> scoop install make
```
This does however require you to first install [Chocolately](https://chocolatey.org/install) or [Scoop](https://scoop.sh/).

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
