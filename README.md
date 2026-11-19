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

Place the dataset at `data/cdnnba.parquet` first:

```sh
curl --fail -L -o data/cdnnba.parquet https://huggingface.co/datasets/Vladislav/nba_dataset/resolve/main/cdnnba.parquet
```

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
make prepare-data
make run-batch
make run-serve
make run-producer
make run-consumer

# Query already-loaded batch results or inspect persisted stream counts
make query-serve
make stream-results

# Run the entire batch and streaming pipeline after Docker is running
make run-pipeline
```

`make docker-run` waits for ClickHouse and Kafka to pass their health checks.
The pipeline prepares Parquet partitioned by season (`YEAR`), runs Spark, loads
its player/game results into ClickHouse, answers all three questions with timings,
then publishes and consumes the Kafka replay. The consumer saves its running
game/period counts in the same ClickHouse database every 10,000 events and at the end.
The producer uses 1,000-row conversion batches and LZ4-compressed Kafka messages
to limit memory and broker storage use. Delivery failures fail the pipeline.

See [Part 3: serving](docs/part3.md) for the SQL, table design, timing method,
replay behavior, and report/demo instructions.

### Docker Desktop recovery

If image downloads fail with `read-only file system` under
`/var/lib/desktop-containerd`, restart Docker Desktop with `docker desktop restart`
and retry `make docker-run`. Check available host disk space before retrying.
If a particular image then fails with `failed to stat parent`, its local snapshot
may be damaged: remove and pull that image again, preserving the named data volumes.
Avoid `make docker-reset` for this repair: it deletes the project's database and Kafka volumes.
