SELECT
    count() AS games,
    sum(field_goals_made) AS total_field_goals,
    round(avg(field_goals_made), 2) AS avg_field_goals_per_game
FROM statline.batch_player_games
WHERE personId = 2544 AND YEAR = 2021
