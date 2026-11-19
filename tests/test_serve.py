"""ClickHouse integration checks. Run after make docker-run; uses an isolated database."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from src.serve.client import execute, query
from src.serve.main import load_batch

SERVE = Path(__file__).resolve().parents[1] / "src/serve"


class BatchExportCompletionTest(unittest.TestCase):
    def test_unfinished_export_is_rejected_before_database_access(self):
        with tempfile.TemporaryDirectory() as directory:
            # A leftover part file must not be mistaken for a complete Spark export.
            (Path(directory) / "part-00000.parquet").touch()
            with (
                patch("src.serve.main.BATCH_RESULTS", Path(directory)),
                patch("src.serve.main.initialize", side_effect=AssertionError("Unexpected database access")) as init,
                patch("src.serve.main.execute", side_effect=AssertionError("Unexpected database access")) as request,
            ):
                with self.assertRaisesRegex(RuntimeError, "missing _SUCCESS"):
                    load_batch()
                init.assert_not_called()
                request.assert_not_called()


class ServeQueriesTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = f"statline_test_{uuid4().hex}"
        schema = (SERVE / "schema.sql").read_text().replace("statline", cls.database)
        execute(schema, multiquery=True)

    @classmethod
    def tearDownClass(cls):
        execute(f"DROP DATABASE {cls.database}")

    def setUp(self):
        execute(f"TRUNCATE TABLE {self.database}.batch_player_games")
        execute(f"TRUNCATE TABLE {self.database}.stream_period_counts")

    def insert(self, table, rows):
        execute(
            f"INSERT INTO {self.database}.{table} FORMAT JSONEachRow",
            data="\n".join(json.dumps(row) for row in rows).encode(),
        )

    def question(self, name):
        sql = (SERVE / "queries" / f"{name}.sql").read_text().replace("statline.", f"{self.database}.")
        return query(sql)["data"]

    def test_zero_make_games_and_season_ranking(self):
        values = [
            (2020, 201939, 1, 10, 7, 10),
            (2021, 201939, 2, 4, 0, 2),
            (2021, 201939, 3, 6, 3, 6),
            (2021, 2544, 4, 0, 0, 0),
            (2021, 2544, 5, 2, 1, 8),
            (2022, 2544, 6, 3, 2, 20),
        ]
        columns = ["YEAR", "personId", "gameId", "three_attempts", "three_makes", "field_goals_made"]
        self.insert("batch_player_games", [dict(zip(columns, row, strict=True)) for row in values])
        self.assertEqual(self.question("q1"), [{"games": 3, "games_with_seven_plus": 1, "percentage": 33.33}])
        self.assertEqual(
            self.question("q2"),
            [
                {"season_start": 2021, "attempts": 10, "makes": 3, "percentage": 30},
                {"season_start": 2020, "attempts": 10, "makes": 7, "percentage": 70},
            ],
        )
        self.assertEqual(self.question("q3"), [{"games": 2, "total_field_goals": 8, "avg_field_goals_per_game": 4}])

    def test_stream_snapshots_do_not_double_count_and_replays_stay_separate(self):
        first, second = str(uuid4()), str(uuid4())
        rows = [
            {"replay_id": first, "gameId": 1, "period": 1, "event_count": 10, "received": 10},
            {"replay_id": first, "gameId": 1, "period": 1, "event_count": 20, "received": 20},
            {"replay_id": first, "gameId": 1, "period": 2, "event_count": 5, "received": 25},
            {"replay_id": second, "gameId": 1, "period": 1, "event_count": 3, "received": 3},
        ]
        # Separate inserts exercise replacement across parts, before background merges.
        for row in rows:
            self.insert("stream_period_counts", [row])
        result = query(f"""
            SELECT replay_id, sum(event_count) AS events
            FROM {self.database}.stream_period_counts FINAL GROUP BY replay_id
        """)["data"]
        self.assertEqual({r["replay_id"]: int(r["events"]) for r in result}, {first: 25, second: 3})


if __name__ == "__main__":
    unittest.main()
