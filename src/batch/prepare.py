"""Prepare the season layout selected by the team's Part 1 experiment."""

from pathlib import Path

from pyspark.sql import SparkSession

RAW = "data/cdnnba.parquet"
STORED = "data/cdnnba_by_season.parquet"


def main() -> None:
    if Path(STORED).is_dir():
        if not (Path(STORED) / "_SUCCESS").is_file():
            raise RuntimeError(
                f"{STORED} has no Spark _SUCCESS marker; check for an incomplete export before reusing it."
            )
        print(f"Using existing season-partitioned dataset: {STORED}", flush=True)
        return
    spark = SparkSession.builder.appName("StatlinePrepareSeasons").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print(f"Preparing {STORED} from {RAW}, partitioned by YEAR...", flush=True)
        spark.read.parquet(RAW).write.mode("errorifexists").partitionBy("YEAR").parquet(STORED)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
