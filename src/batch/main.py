from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Season date sources:
# 2020-2021: https://en.wikipedia.org/wiki/2020–21_NBA_season
# 2021-2022: https://en.wikipedia.org/wiki/2021–22_NBA_season
# 2022-2023: https://en.wikipedia.org/wiki/2022–23_NBA_season
# 2023-2024: https://en.wikipedia.org/wiki/2023–24_NBA_season
# 2024-2025: https://en.wikipedia.org/wiki/2024–25_NBA_season

SEASONS_BOUNDARIES = {
    "2020-2021": ("2020-12-22T00:00:00Z", "2021-07-21T00:00:00Z"),
    "2021-2022": ("2021-10-19T00:00:00Z", "2022-06-17T00:00:00Z"),
    "2022-2023": ("2022-10-18T00:00:00Z", "2023-06-13T00:00:00Z"),
    "2023-2024": ("2023-10-24T00:00:00Z", "2024-06-18T00:00:00Z"),
    "2024-2025": ("2024-10-22T00:00:00Z", "2025-06-23T00:00:00Z"),
}

# Useful for answering two specfied questions from this step
STEPH_CURRY_ID = 201939
LEBRON_JAMES_ID = 2544

# Spark instance
spark = SparkSession.builder.appName("BatchPart1").master("local[*]").getOrCreate()

# Sanity while debugging
spark.sparkContext.setLogLevel("ERROR")

# Read in raw parquet file
df = spark.read.parquet("data/cdnnba.parquet")

# Create CSV for comparison
df.write.mode("overwrite").option("header", True).csv("data/cdnnba_csv")


def size_mb(path):
    path = Path(path)

    if path.is_file():
        total_bytes = path.stat().st_size
    else:
        total_bytes = sum(file.stat().st_size for file in path.rglob("*") if file.is_file())

    return total_bytes / 1_000_000


# Minimum Functionality

# Answer Q1: Based on his past performances, should I bet on Steph Curry making 7 three’s tonight?


def q1(df):
    print()
    print("Spark Job for Q1: START")
    # Use const since player with same playerNameI exists: "Seth Curry"
    # Data does not give great resources to determine who is who reliably
    player_events = df.filter(F.col("personId") == STEPH_CURRY_ID)

    is_three = F.col("actionType") == "3pt"
    is_made = F.col("shotResult") == "Made"

    three_stats_per_game = player_events.groupBy("gameId").agg(
        F.sum(F.when(is_three, 1).otherwise(0)).alias("three_attempts"),
        F.sum(F.when(is_three & is_made, 1).otherwise(0)).alias("three_makes"),
    )

    total_games = three_stats_per_game.count()
    games_with_seven_plus = three_stats_per_game.filter(F.col("three_makes") >= 7).count()

    percentage = games_with_seven_plus / total_games * 100 if total_games > 0 else None

    print(f"Steph Curry made 7 or more threes in {percentage:.2f}% in all his games played from 2020-2025")
    print("Chances of your bet hitting is unlikely!")

    print()
    print("Spark Job for Q1: END")


# Answer Q3: How many field goals did LeBron James make per game, on average, in the 2021-2022 season?


def q3(df):
    print()
    print("Spark Job for Q3: START")
    start, end = SEASONS_BOUNDARIES["2021-2022"]

    player_events = df.filter(F.col("personId") == LEBRON_JAMES_ID)
    player_events_in_season = player_events.filter((F.col("timeActual") >= start) & (F.col("timeActual") < end))

    is_field_goal = F.col("actionType").isin("2pt", "3pt")
    is_made = F.col("shotResult") == "Made"

    field_goals_per_game = player_events_in_season.groupBy("gameId").agg(
        F.sum(F.when(is_field_goal & is_made, 1).otherwise(0)).alias("field_goals_made")
    )

    result = field_goals_per_game.agg(F.avg("field_goals_made").alias("avg_field_goals_per_game")).first()

    average = result["avg_field_goals_per_game"]

    print(f"Lebron James made an average of {average:.0f} field goals in all his games played in the 2021-2022 season")

    print()
    print("Spark Job for Q3: END")


"""
TODO:

Deep:
  - Convert Parquet raw file to a CSV DONE
  - Create other Parquet file partitioned by season
    - Subtask: Find beginning and ends of each season btwn. 2020-2025
    - Use spark to partition by these start and end dates
    - Write this somewhere
  - Benchmarking
    - Compare size of all 3
    - Compare read time of all 3
      - Use our original queries to get this done
"""


def batch_job() -> None:
    # Storage Format Comparisons
    print()
    print("Storage Format Comparison: START")
    print("Chosen storage format is Parquet.")
    print("Compare File Sizes:")
    print(f"Parquet size: {size_mb('data/cdnnba.parquet'):.2f} MB")
    print(f"CSV size: {size_mb('data/cdnnba_csv'):.2f} MB")
    print("Storage Format Comparison: END")
    print()

    # Spark Job Questions
    q1(df)
    q3(df)


if __name__ == "__main__":
    batch_job()
