"""Create a known set of invalid rows for measuring rule effectiveness."""

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

INJECTED_RULE_COLUMN = "_injected_rule"


def _null_as(df: DataFrame, column: str) -> Column:
    return F.lit(None).cast(df.schema[column].dataType)


def _as_type(df: DataFrame, column: str, value: object) -> Column:
    return F.lit(value).cast(df.schema[column].dataType)


def inject_known_bad_rows(df: DataFrame) -> DataFrame:
    """Append one deterministic bad row for each validation rule.

    A single source event is copied five times. Each copy is changed so that it
    violates one known rule and is labelled with the rule expected to catch it.
    """
    if not df.take(1):
        raise ValueError("Cannot inject bad rows into an empty DataFrame")

    source = df.limit(1)
    mutations: tuple[tuple[str, Callable[[DataFrame], DataFrame]], ...] = (
        (
            "missing_game_id",
            lambda row: row.withColumn("gameId", _null_as(df, "gameId")),
        ),
        (
            "invalid_action_number",
            lambda row: row.withColumn("actionNumber", _as_type(df, "actionNumber", -1)),
        ),
        (
            "invalid_time_actual",
            lambda row: row.withColumn("timeActual", _null_as(df, "timeActual")),
        ),
        (
            "invalid_period",
            lambda row: row.withColumn("period", _as_type(df, "period", 0)),
        ),
        (
            "invalid_shot_result",
            lambda row: row.withColumn("actionType", _as_type(df, "actionType", "3pt")).withColumn(
                "shotResult",
                _as_type(df, "shotResult", "__INVALID__"),
            ),
        ),
    )

    result = df.withColumn(INJECTED_RULE_COLUMN, F.lit(None).cast("string"))
    for expected_rule, mutate in mutations:
        bad_row = mutate(source).withColumn(INJECTED_RULE_COLUMN, F.lit(expected_rule))
        result = result.unionByName(bad_row)

    return result
