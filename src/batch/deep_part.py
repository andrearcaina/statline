"""Compare storage sizes and query runtimes on the same NBA events."""

import argparse
from pathlib import Path
from statistics import mean
from time import perf_counter

from main import q1, q3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW = Path("data/cdnnba.parquet")
CSV = Path("data/cdnnba_csv")
SEASON_PARQUET = Path("data/cdnnba_by_season.parquet")
SEASON_LAYOUT = "Season Parquet"
# Handle quoted fields that span lines, and fail if a row cannot be parsed.
CSV_OPTIONS = {"header": "true", "multiLine": "true", "escape": '"', "mode": "FAILFAST"}


def scan_latest(df):
    # Include every source column in the result so the scan cannot prune columns.
    # Sort columns because Spark can append YEAR after discovering it from the directory.
    # JSON gives a stable final tie-breaker without depending on file order.
    row = F.to_json(F.struct(*sorted(df.columns)))
    # Spark compares the fields in order. Game and action IDs break timestamp ties.
    key = F.struct(
        F.try_to_timestamp("timeActual"),
        F.col("gameId"),
        F.col("actionNumber"),
        row,
    )
    # Return the row count and latest event in one Spark action.
    return df.agg(F.count("*").alias("rows"), F.max(key).alias("latest")).first()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prepare-csv",
        action="store_true",
        help="Write CSV from raw Parquet before measuring.",
    )
    parser.add_argument(
        "--prepare-partitioned",
        action="store_true",
        help="Write Parquet grouped by season before measuring.",
    )
    parser.add_argument("--sizes-only", action="store_true", help="Print sizes without running queries.")
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be at least 1")
    if not args.prepare_csv and not CSV.exists():
        parser.error("CSV is missing. Run with --prepare-csv first.")

    if not args.prepare_partitioned and not SEASON_PARQUET.exists():
        parser.error("Season-partitioned Parquet is missing. Run with --prepare-partitioned first.")

    spark = SparkSession.builder.appName("StatlineDeepPart").config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        # Reuse the Parquet column types so CSV reads skip schema inference.
        raw_schema = spark.read.parquet(str(RAW)).schema
        # Create the CSV before any timed runs. Spark refuses to overwrite an existing export.
        if args.prepare_csv:
            print("Preparing CSV from raw Parquet...", flush=True)
            (
                spark.read.parquet(str(RAW))
                .write.mode("errorifexists")
                .options(
                    header=True,
                    escape='"',
                    ignoreLeadingWhiteSpace=False,
                    ignoreTrailingWhiteSpace=False,
                )
                .csv(str(CSV))
            )

        if args.prepare_partitioned:
            print("Preparing Parquet partitioned by season (YEAR)...", flush=True)
            # YEAR is the season's starting year. Spark writes directories such as YEAR=2021.
            # The partitioned copy contains all source rows and shares the raw Parquet schema.
            (spark.read.parquet(str(RAW)).write.mode("errorifexists").partitionBy("YEAR").parquet(str(SEASON_PARQUET)))

        # Each function opens its layout again for every benchmark run.
        baseline = "Raw Parquet"
        layouts = {
            baseline: lambda: spark.read.schema(raw_schema).parquet(str(RAW)),
            "CSV": lambda: spark.read.schema(raw_schema).options(**CSV_OPTIONS).csv(str(CSV)),
            # Read from the root so Spark restores YEAR from the partition directories.
            SEASON_LAYOUT: lambda: spark.read.schema(raw_schema).parquet(str(SEASON_PARQUET)),
        }
        # Count only data files. Spark also writes markers such as _SUCCESS.
        print("\nStorage sizes", flush=True)
        sizes = {}
        for name, files in (
            (baseline, [RAW]),
            ("CSV", sorted(CSV.glob("part-*.csv"))),
            (SEASON_LAYOUT, sorted(SEASON_PARQUET.rglob("part-*.parquet"))),
        ):
            sizes[name] = sum(file.stat().st_size for file in files) / 1024**2
            print(f"  {name:<16} {sizes[name]:>9,.2f} MiB ({len(files)} data files)")
        for name in ("CSV", SEASON_LAYOUT):
            size_change = (sizes[name] / sizes[baseline] - 1) * 100
            print(f"  {name} size vs. raw Parquet: {size_change:+.1f}%")
        if args.sizes_only:
            return

        workloads = (
            ("Full scan", "Find the latest event and count all rows across all seasons.", scan_latest),
            ("Q1", "How often did Steph Curry make at least 7 threes in a game?", q1),
            ("Q3", "How many field goals did LeBron James make per game in 2021–2022?", q3),
        )
        comparisons = []
        for label, description, operation in workloads:
            heading = "Full scan" if operation == scan_latest else f"Answering {label}"
            print(f"\n{heading}: {description}", flush=True)
            if operation == q1:
                print("  Uses all seasons in the dataset.", flush=True)
            elif operation == q3:
                print("  Uses only the 2021–2022 season.", flush=True)

            # Get a reference answer from the source Parquet before timing.
            print("  Checking the reference result from raw Parquet...", flush=True)
            expected = operation(layouts[baseline]())
            timings = {name: [] for name in layouts}
            for repeat in range(args.runs):
                names = list(layouts)
                # Rotate the order so each layout goes first once in the default three runs.
                offset = repeat % len(names)
                names = names[offset:] + names[:offset]
                for name in names:
                    print(f"  Run {repeat + 1}/{args.runs} · {name}...", flush=True)
                    # Time from opening the files through Spark returning the result.
                    start = perf_counter()
                    result = operation(layouts[name]())
                    elapsed = perf_counter() - start
                    timings[name].append(elapsed)
                    if result != expected:
                        raise ValueError(f"{name} returned a different result for {label}")
                    print(f"    Finished in {elapsed:.3f} s", flush=True)

            if operation == scan_latest:
                print(f"  Verified: {expected.rows:,} rows and the same latest event.")
            elif operation == q1 and expected.games:
                percentage = 100 * expected.games_with_seven_plus / expected.games
                print(
                    f"  Verified: {expected.games_with_seven_plus:,} of {expected.games:,} games ({percentage:.2f}%)."
                )
            elif operation == q3 and expected.games:
                print(
                    f"  Verified: {expected.avg_field_goals_per_game:.2f} made field goals per game "
                    f"({expected.total_field_goals:,} makes across {expected.games:,} games)."
                )
            else:
                print("  Verified: no matching games in any layout.")

            raw_average = mean(timings[baseline])
            for name, durations in timings.items():
                average = mean(durations)
                change = (average / raw_average - 1) * 100
                comparisons.append((label, name, average, change))

        print(f"\nAverage runtime comparison (timed runs per layout: {args.runs})")
        print(f"  {'Workload':<12} {'Layout':<16} {'Average':>10} {'vs. Raw Parquet':>16}")
        for label, name, average, change in comparisons:
            difference = "baseline" if name == baseline else f"{change:+.1f}%"
            print(f"  {label:<12} {name:<16} {average:>8.3f} s {difference:>16}")
        print("  Positive percentages mean longer runtimes.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
