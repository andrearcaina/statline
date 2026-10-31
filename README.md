# statline

nba play by play stats data pipeline

### How to run it locally

Make sure you have `gnu make`, [`uv`](https://github.com/astral-sh/uv) and [`docker`](https://www.docker.com/get-started/) installed.

Then, run the following:

```sh
# install dependencies, fmt, lint, validate
> make check

# start spark, clickhouse, and kafka on docker
> make docker-run

# run each pipeline entrypoint locally
> make run-batch
> make run-producer
> make run-consumer

# run the entire pipeline
> make run-pipeline
```
