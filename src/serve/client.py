"""Use the existing local ClickHouse container without extra client dependencies."""

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def execute(sql: str, *, data: bytes | None = None, multiquery: bool = False) -> str:
    command = ["docker", "compose", "exec", "-T", "clickhouse", "clickhouse-client"]
    if multiquery:
        command.append("--multiquery")
    command.extend(["--query", sql])
    result = subprocess.run(command, input=data, capture_output=True, cwd=ROOT, timeout=120)
    if result.returncode:
        raise RuntimeError(f"ClickHouse request failed: {result.stderr.decode().strip()}")
    return result.stdout.decode()


def query(sql: str) -> dict:
    return json.loads(execute(f"{sql.rstrip().rstrip(';')} FORMAT JSON"))


def insert(table: str, rows: list[dict]) -> None:
    if table not in {"batch_player_games_staging", "stream_period_counts"}:
        raise ValueError(f"Unsupported insert table: {table}")
    if rows:
        data = "\n".join(json.dumps(row, allow_nan=False) for row in rows).encode()
        execute(f"INSERT INTO statline.{table} FORMAT JSONEachRow", data=data)


def initialize() -> None:
    execute(Path(__file__).with_name("schema.sql").read_text(), multiquery=True)
