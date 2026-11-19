-- "Worst" means lowest three-point percentage within the supplied dataset.
-- Display all seasons, worst first; YEAR is the season's starting year.
SELECT
    YEAR AS season_start,
    sum(three_attempts) AS attempts,
    sum(three_makes) AS makes,
    round(100.0 * makes / nullIf(attempts, 0), 2) AS percentage
FROM statline.batch_player_games
WHERE personId = 201939
GROUP BY YEAR
HAVING attempts > 0
ORDER BY makes / attempts, season_start
