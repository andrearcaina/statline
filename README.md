# statline
nba/wnba play by play stats data analytics platform

### How to run it locally

Make sure you have `gnu make`, [`uv`](https://github.com/astral-sh/uv) and [`docker`](https://www.docker.com/get-started/) installed.

Then, run the following:

```sh
# install dependencies, fmt, lint, validate
> make check

# run fastapi app locally
> make run 

# run fastapi app, clickhouse, and kafka on docker
> make docker-run
```

