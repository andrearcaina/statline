# Part 3: Serve

The assignment's minimum is to load batch and stream results into one database,
answer all three user questions there, and measure each response time. No web API
is required. The team's deep dive remains Part 1's storage experiment.

## Data flow

1. `make prepare-data` creates `data/cdnnba_by_season.parquet` from the original
   file using Spark's `partitionBy("YEAR")`. It reuses a completed existing export.
   This is the same season layout as Kyle's experiment; YEAR is the starting year.
2. `make run-batch` reads that stored layout and exports player/game shooting
   totals to `data/batch_player_games.parquet`, while preserving the existing
   Spark Q1 and Q3 output.
3. `make run-serve` loads the Spark output into ClickHouse and measures Q1, Q2,
   and Q3. The loader validates its read/write row counts and swaps in a completed
   staging table atomically. Repeated loads replace the snapshot instead of
   appending duplicate player/game rows. Run only one batch loader at a time.
4. The producer reads the same season layout and sorts by `timeActual` before
   publishing to Kafka. The consumer keeps the existing running count by game and
   period, saving changed counts to ClickHouse every 10,000 events and at replay end.
   It saves before committing offsets; database failures stop the consumer.
5. `make stream-results` queries the stored running counts, one summary per
   consumer invocation, with counts and the latest update time.

`make run-pipeline` executes these steps in order. From a fresh clone with the
documented prerequisites installed, download the dataset and run `make run`
(two commands; the full run installs dependencies and starts services).

## ClickHouse tables

`src/serve/schema.sql` defines both results in the `statline` database:

| Table | Grain | Design |
| --- | --- | --- |
| `batch_player_games` | Player, game, and season | MergeTree, partitioned by YEAR, ordered by `(personId, YEAR, gameId)` |
| `stream_period_counts` | Consumer invocation, game, and period | ReplacingMergeTree with `received` as the snapshot version |

The batch sort key serves the player filter shared by all three questions and
the season filter in Q3. The columns are season, numeric player/game IDs, three-point
attempts/makes, and made field goals. Games with zero made shots are retained.
This is a compact serving result, not a second copy of every raw event.

Use `FINAL` when reading the stream table, as the provided stream query does:
successive counts are snapshots, not increments to add together. Each consumer
invocation has a new `replay_id`, so separate replays are not combined. In-memory
counts are not restored after a crash: a restarted consumer creates a separate,
potentially partial observation from its committed Kafka offsets. This implements
the assignment's minimum, not exactly-once recovery or the streaming deep dive.

## Questions and measurement

The executable SQL lives in `src/serve/queries/`:

- **Q1:** Historical fraction of Curry games with at least seven made threes.
  The denominator is games with any Curry event, matching the existing Spark logic,
  including zero-make games. This does not predict tonight's outcome or evaluate odds.
- **Q2:** Curry seasons ranked from lowest to highest three-point percentage,
  with attempts and makes shown. "Worst" is defined as the lowest percentage;
  `season_start = 2021` means 2021-2022. Seasons with no attempts are excluded.
- **Q3:** LeBron's average made two- and three-point field goals per game in the
  season starting in 2021. Zero-make games remain in the denominator.

All supplied events are included; no regular-season/playoff filter is applied.
This preserves the dataset scope used by the team's existing Spark results.

`make query-serve` measures each query three times and saves individual timings,
means, answers, version, and timestamp to `data/serve_measurements.json`:

- `response_ms` includes Python subprocess/Docker CLI/client startup, query
  execution, and returned results. It excludes the batch calculation and load.
- `server_ms` is ClickHouse's own elapsed time from its JSON statistics.

These are repeated local runs with caches left in place, not cold-cache timings.
Measurements are environment-specific. The implementation does not claim a
before/after indexing speedup; that comparison is optional for Part 3, while the
team's measured deep dive is Part 1.

## Verified local run (November 19, 2026)

The prepared season layout contained all 3,337,750 original rows. Spark produced
128,329 player/game result rows. Loading twice left the same row count and unique
keys. The completed stream run stored 3,337,750 events across 24,349 game/period
groups; every individual group's count matched the original source.

Measured with ClickHouse 25.8.33.6, three runs per query:

| Question | Database answer | Mean response (ms) | Mean server (ms) |
| --- | --- | ---: | ---: |
| Q1 | 74 of 327 games, 22.63% | 93.246 | 4.023 |
| Q2 | 2021-2022, 285/748, 38.10% | 95.569 | 4.261 |
| Q3 | 640 makes / 56 games, 11.43 per game | 88.940 | 2.206 |

These are one local measurement set, not guaranteed timings. Regenerate the JSON
for the final recording and use the exact results from that run in the report.

## Report and demo

After `make docker-run`, use `make run-pipeline` to show Spark output, database
answers/timings, Kafka progress, and saved stream totals. In another terminal,
`make stream-results` shows counts growing while the consumer runs. The batch
load and the stream snapshots remain separate results in one database, preventing
the replay from double-counting the historical shooting totals.

For direct SQL access:

```sh
docker compose exec clickhouse clickhouse-client --database statline
```

Run `SELECT count() FROM batch_player_games;` and the SQL files' SELECT statements.
For stream results, use `SELECT * FROM stream_period_counts FINAL LIMIT 10;`.

For the database comparison in the report: ClickHouse fits grouped analytical
reads and column-oriented storage. PostgreSQL is a reasonable alternative for
transactional updates and conventional constraints, but this project mainly
serves aggregated analytical questions. This is an architectural rationale,
not a measured comparison between the databases.

The architecture diagram should now show Spark exporting aggregates that the
serving loader inserts into ClickHouse, and the Kafka consumer inserting its
running counts into the same database. Mention using YEAR instead of deriving
seasons from timestamps as a change since the proposal. Part 4's checks,
quarantine, and complete stage monitoring remain the quality owner's work.

Run integration checks with services up:

```sh
uv run python -m unittest discover -s tests -v
```

Tests use their own temporary database and cover zero-make games, season ranking,
season filtering, and avoiding double counting successive stream snapshots.
Keep the team's AI acknowledgement current: AI assisted with the Docker repair,
serving implementation, SQL, documentation, and verification.
