"""Executable smoke test proving that each injected bad row is detected."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from src.quality.inject_bad_rows import INJECTED_RULE_COLUMN, inject_known_bad_rows
from src.quality.rules import IS_VALID_COLUMN, QUALITY_ERRORS_COLUMN, validate_events


def main() -> None:
    spark = SparkSession.builder.appName("StatlineQualityTest").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType(
        [
            StructField("gameId", LongType(), True),
            StructField("actionNumber", IntegerType(), True),
            StructField("timeActual", StringType(), True),
            StructField("period", IntegerType(), True),
            StructField("actionType", StringType(), True),
            StructField("shotResult", StringType(), True),
        ]
    )
    rows = [
        (1001, 1, "2024-01-01T20:00:00Z", 1, "3pt", "Made"),
        # Overtime is legitimate and must not be rejected by the period rule.
        (1001, 200, "2024-01-01T22:30:00Z", 5, "2pt", "Missed"),
    ]

    try:
        events = spark.createDataFrame(rows, schema)
        checked = validate_events(inject_known_bad_rows(events)).cache()

        unexpected_good_row_failures = checked.filter(
            F.col(INJECTED_RULE_COLUMN).isNull() & ~F.col(IS_VALID_COLUMN)
        ).count()
        assert unexpected_good_row_failures == 0

        injected = checked.filter(F.col(INJECTED_RULE_COLUMN).isNotNull())
        assert injected.count() == 5

        missed = injected.filter(~F.array_contains(F.col(QUALITY_ERRORS_COLUMN), F.col(INJECTED_RULE_COLUMN))).count()
        assert missed == 0

        results = injected.select(
            INJECTED_RULE_COLUMN,
            QUALITY_ERRORS_COLUMN,
        ).orderBy(INJECTED_RULE_COLUMN)
        results.show(truncate=False)
        print("PASS: all 5 injected bad rows were caught; both known-good rows passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
