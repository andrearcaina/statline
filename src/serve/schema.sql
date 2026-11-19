CREATE DATABASE IF NOT EXISTS statline ENGINE = Atomic;

-- One row per player/game, produced by Spark from the season-partitioned data.
CREATE TABLE IF NOT EXISTS statline.batch_player_games
(
    YEAR UInt16,
    personId UInt64,
    gameId UInt64,
    three_attempts UInt32,
    three_makes UInt32,
    field_goals_made UInt32
)
ENGINE = MergeTree
PARTITION BY YEAR
ORDER BY (personId, YEAR, gameId);

CREATE TABLE IF NOT EXISTS statline.batch_player_games_staging
AS statline.batch_player_games;

-- A replay is a separate observation. FINAL selects its newest running count.
CREATE TABLE IF NOT EXISTS statline.stream_period_counts
(
    replay_id UUID,
    gameId UInt64,
    period UInt16,
    event_count UInt64,
    received UInt64,
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(received)
ORDER BY (replay_id, gameId, period);
