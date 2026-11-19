"""Spark expressions used to validate NBA play-by-play events."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

QUALITY_ERRORS_COLUMN = "_quality_errors"
IS_VALID_COLUMN = "_is_valid"

REQUIRED_COLUMNS = {
    "gameId",
    "actionNumber",
    "timeActual",
    "period",
    "actionType",
    "shotResult",
}


def validate_events(df: DataFrame) -> DataFrame:
    """Add validation results without dropping or changing source rows."""
    missing_columns = sorted(REQUIRED_COLUMNS - set(df.columns))
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"Cannot validate events; missing columns: {missing}")

    is_shot = F.col("actionType").isin("2pt", "3pt")
    has_valid_shot_result = F.col("shotResult").isin("Made", "Missed")

    possible_errors = F.array(
        F.when(F.col("gameId").isNull(), F.lit("missing_game_id")),
        F.when(
            F.col("actionNumber").isNull() | (F.col("actionNumber") < 0),
            F.lit("invalid_action_number"),
        ),
        F.when(
            F.col("timeActual").isNull() | F.try_to_timestamp("timeActual").isNull(),
            F.lit("invalid_time_actual"),
        ),
        F.when(
            F.col("period").isNull() | (F.col("period") < 1),
            F.lit("invalid_period"),
        ),
        F.when(
            is_shot & ~has_valid_shot_result,
            F.lit("invalid_shot_result"),
        ),
    )

    quality_errors = F.filter(possible_errors, lambda error: error.isNotNull())
    return df.withColumn(QUALITY_ERRORS_COLUMN, quality_errors).withColumn(
        IS_VALID_COLUMN,
        F.size(F.col(QUALITY_ERRORS_COLUMN)) == 0,
    )
