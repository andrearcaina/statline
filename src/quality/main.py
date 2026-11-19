"""Validate NBA events, quarantine failures, and produce a quality report."""

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.quality.inject_bad_rows import INJECTED_RULE_COLUMN, inject_known_bad_rows
from src.quality.rules import IS_VALID_COLUMN, QUALITY_ERRORS_COLUMN, validate_events


def quality_job(input_path: str, output_path: str, inject_bad_rows: bool) -> dict[str, object]:
    spark = SparkSession.builder.appName("StatlineQuality").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        events = spark.read.parquet(input_path)
        if inject_bad_rows:
            events = inject_known_bad_rows(events)
        elif INJECTED_RULE_COLUMN not in events.columns:
            events = events.withColumn(INJECTED_RULE_COLUMN, F.lit(None).cast("string"))

        # Do not cache the wide source dataset: the local Spark container has a
        # small heap, and materializing every column can exhaust it.
        checked = validate_events(events)
        summary = checked.agg(
            F.count("*").alias("total_rows"),
            F.sum(F.col(IS_VALID_COLUMN).cast("long")).alias("valid_rows"),
            F.sum((~F.col(IS_VALID_COLUMN)).cast("long")).alias("quarantined_rows"),
            F.sum(F.col(INJECTED_RULE_COLUMN).isNotNull().cast("long")).alias("injected_rows"),
            F.sum(
                (
                    F.col(INJECTED_RULE_COLUMN).isNotNull()
                    & F.array_contains(F.col(QUALITY_ERRORS_COLUMN), F.col(INJECTED_RULE_COLUMN))
                ).cast("long")
            ).alias("injected_rows_caught"),
            F.sum((F.col(INJECTED_RULE_COLUMN).isNull() & ~F.col(IS_VALID_COLUMN)).cast("long")).alias(
                "real_rows_flagged"
            ),
        ).first()

        rule_counts = {
            row["rule"]: row["count"]
            for row in (
                checked.select(F.explode(QUALITY_ERRORS_COLUMN).alias("rule"))
                .groupBy("rule")
                .count()
                .orderBy("rule")
                .collect()
            )
        }

        output = Path(output_path)
        output.mkdir(parents=True, exist_ok=True)
        checked.filter(~F.col(IS_VALID_COLUMN)).write.mode("overwrite").parquet(str(output / "quarantine.parquet"))

        report = {
            "input": input_path,
            "bad_rows_injected": inject_bad_rows,
            "total_rows": summary.total_rows,
            "valid_rows": summary.valid_rows,
            "quarantined_rows": summary.quarantined_rows,
            "injected_rows": summary.injected_rows,
            "injected_rows_caught": summary.injected_rows_caught,
            "real_rows_flagged": summary.real_rows_flagged,
            "failures_by_rule": rule_counts,
            "note": ("Real rows flagged require manual review; they are not automatically false positives."),
        }
        (output / "quality_report.json").write_text(
            json.dumps(report, indent=2) + "\n",
            encoding="utf-8",
        )
        return report
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default="data/cdnnba.parquet")
    parser.add_argument("--output", default="data/quality")
    parser.add_argument(
        "--inject-known-bad",
        action="store_true",
        help="Append five labelled invalid events before validation.",
    )
    args = parser.parse_args()

    report = quality_job(args.input, args.output, args.inject_known_bad)
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
