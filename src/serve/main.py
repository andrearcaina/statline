"""Load Spark outputs and measure the three user-facing ClickHouse queries."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from statistics import mean
from time import perf_counter

import pyarrow.parquet as pq

from .client import ROOT, execute, initialize, insert, query

QUERIES = Path(__file__).with_name("queries")
BATCH_RESULTS = ROOT / "data/batch_player_games.parquet"


def load_batch() -> None:
    if not BATCH_RESULTS.is_dir():
        raise FileNotFoundError("Spark results missing. Run make run-batch first.")
    initialize()
    # Load a staging table, then publish the complete snapshot atomically.
    # A failed load leaves the last successful snapshot available to readers.
    execute("TRUNCATE TABLE statline.batch_player_games_staging")
    dataset = pq.ParquetDataset(BATCH_RESULTS)
    loaded = 0
    for fragment in dataset.fragments:
        for batch in fragment.to_batches(batch_size=10_000):
            rows = batch.to_pylist()
            insert("batch_player_games_staging", rows)
            loaded += len(rows)
    actual = int(query("SELECT count() AS rows FROM statline.batch_player_games_staging")["data"][0]["rows"])
    if not loaded or actual != loaded:
        raise ValueError(f"Batch load count mismatch: {loaded} read, {actual} stored")
    execute("EXCHANGE TABLES statline.batch_player_games AND statline.batch_player_games_staging")
    execute("TRUNCATE TABLE statline.batch_player_games_staging")
    print(f"Batch results loaded: {loaded:,} player/game rows into ClickHouse.", flush=True)


def answer_questions(runs: int) -> dict:
    batch_rows = int(query("SELECT count() AS rows FROM statline.batch_player_games")["data"][0]["rows"])
    if batch_rows == 0:
        raise ValueError("No batch results loaded. Run make run-serve first.")
    report = {
        "measured_at_utc": datetime.now(UTC).isoformat(),
        "clickhouse_version": query("SELECT version() AS version")["data"][0]["version"],
        "batch_rows": batch_rows,
        "source": "data/cdnnba_by_season.parquet via Spark player/game aggregates",
        "runs": runs,
        "scope": "All supplied events; no regular-season/playoff exclusion",
        "timing_scope": "response_ms includes Docker CLI startup and transfer; server_ms is ClickHouse elapsed time",
        "queries": {},
    }
    for name in ("q1", "q2", "q3"):
        sql = (QUERIES / f"{name}.sql").read_text()
        timings = []
        server_timings = []
        expected = None
        for _ in range(runs):
            start = perf_counter()
            result = query(sql)
            timings.append((perf_counter() - start) * 1_000)
            server_timings.append(result["statistics"]["elapsed"] * 1_000)
            if expected is not None and expected != result["data"]:
                raise ValueError(f"{name} changed between measurements")
            expected = result["data"]
        report["queries"][name] = {
            "answer": expected,
            "response_ms": timings,
            "mean_response_ms": mean(timings),
            "server_ms": server_timings,
            "mean_server_ms": mean(server_timings),
        }
        print(f"\n{name.upper()}: {json.dumps(expected)}")
        print(f"  Mean response: {mean(timings):.3f} ms; ClickHouse server: {mean(server_timings):.3f} ms")
    print("Response time includes Docker CLI/client startup and result transfer; server time is reported separately.")
    print("Repeated local runs; caches are not cleared. Q1 is historical frequency, not a betting forecast.")
    return report


def show_stream() -> None:
    result = query("""
        SELECT replay_id, sum(event_count) AS events, count() AS game_periods,
               max(received) AS received, max(updated_at) AS latest_update
        FROM statline.stream_period_counts FINAL
        GROUP BY replay_id ORDER BY latest_update DESC
    """)
    print(json.dumps(result["data"], indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["init", "load", "query", "stream"])
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    if args.command == "load":
        load_batch()
    elif args.command == "query":
        report = answer_questions(args.runs)
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
    elif args.command == "stream":
        show_stream()
    else:
        initialize()


if __name__ == "__main__":
    main()
