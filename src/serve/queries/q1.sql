-- All games in which Curry has any event, including games with zero made threes.
SELECT
    count() AS games,
    countIf(three_makes >= 7) AS games_with_seven_plus,
    round(100.0 * games_with_seven_plus / nullIf(games, 0), 2) AS percentage
FROM statline.batch_player_games
WHERE personId = 201939
