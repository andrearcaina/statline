from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Useful for answering two specfied questions from this step
STEPH_CURRY_ID = 201939
LEBRON_JAMES_ID = 2544

# Answer Q1: Based on his past performances, should I bet on Steph Curry making 7 three’s tonight?


def q1(df):
    # Use const since player with same playerNameI exists: "Seth Curry"
    # Data does not give great resources to determine who is who reliably
    player_events = df.filter(F.col("personId") == STEPH_CURRY_ID)

    is_three = F.col("actionType") == "3pt"
    is_made = F.col("shotResult") == "Made"

    three_stats_per_game = player_events.groupBy("gameId").agg(
        F.sum(F.when(is_three, 1).otherwise(0)).alias("three_attempts"),
        F.sum(F.when(is_three & is_made, 1).otherwise(0)).alias("three_makes"),
    )

    return three_stats_per_game.agg(
        F.count("*").alias("games"),
        F.count(F.when(F.col("three_makes") >= 7, 1)).alias("games_with_seven_plus"),
    ).first()


# Answer Q3: How many field goals did LeBron James make per game, on average, in the 2021-2022 season?


def q3(df):
    player_events = df.filter(F.col("personId") == LEBRON_JAMES_ID)
    # YEAR stores the season start. This filter lets Spark skip other season directories.
    player_events_in_season = player_events.filter(F.col("YEAR") == 2021)

    is_field_goal = F.col("actionType").isin("2pt", "3pt")
    is_made = F.col("shotResult") == "Made"

    field_goals_per_game = player_events_in_season.groupBy("gameId").agg(
        F.sum(F.when(is_field_goal & is_made, 1).otherwise(0)).alias("field_goals_made")
    )

    return field_goals_per_game.agg(
        F.count("*").alias("games"),
        F.sum("field_goals_made").alias("total_field_goals"),
        F.avg("field_goals_made").alias("avg_field_goals_per_game"),
    ).first()


def batch_job() -> None:
    print("=== Statline batch job ===", flush=True)
    print("Starting Spark...", flush=True)
    spark = SparkSession.builder.appName("BatchPart1").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print("Opening data/cdnnba.parquet...", flush=True)
        df = spark.read.parquet("data/cdnnba.parquet")

        print("\nQ1 | How often did Steph Curry make at least 7 threes in a game?", flush=True)
        print("  Scope: all seasons in the dataset.")
        print("  Counting made threes per game in Spark...", flush=True)
        curry = q1(df)
        if curry.games:
            percentage = 100 * curry.games_with_seven_plus / curry.games
            print(
                f"  Result: {curry.games_with_seven_plus:,} of {curry.games:,} games ({percentage:.2f}%).", flush=True
            )
        else:
            print("  No Steph Curry games found.", flush=True)

        print("\nQ3 | How many field goals did LeBron James make per game in 2021-2022?", flush=True)
        print("  Scope: 2021-2022 season. Includes made two-pointers and three-pointers.")
        print("  Counting made field goals and averaging across games in Spark...", flush=True)
        lebron = q3(df)
        if lebron.games:
            print(
                f"  Result: {lebron.avg_field_goals_per_game:.2f} made field goals per game "
                f"({lebron.total_field_goals:,} makes across {lebron.games:,} games).",
                flush=True,
            )
        else:
            print("  No LeBron James games found for 2021-2022.", flush=True)

        print("\n=== Batch job complete ===", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    batch_job()
