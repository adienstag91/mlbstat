-- Simple Validation Queries
-- ========================

-- 1. Games with diffs by year
SELECT 
    EXTRACT(YEAR FROM g.game_date) AS year,
    COUNT(DISTINCT vr.game_id) AS total_games,
    COUNT(DISTINCT vr.game_id) FILTER (WHERE vr.accuracy_percentage < 100.0) AS games_with_diffs,
    SUM(vr.discrepancies_count) FILTER (WHERE vr.validation_type = 'batting') AS total_batting_diffs,
    SUM(vr.discrepancies_count) FILTER (WHERE vr.validation_type = 'pitching') AS total_pitching_diffs
FROM validation_reports vr
JOIN games g ON vr.game_id = g.game_id
GROUP BY EXTRACT(YEAR FROM g.game_date)
ORDER BY year DESC;


-- 2. List of all games with diffs
SELECT 
    g.game_id,
    g.date,
    g.home_team,
    g.away_team,
    MAX(CASE WHEN vr.validation_type = 'batting' THEN vr.accuracy_percentage END) AS batting_accuracy,
    MAX(CASE WHEN vr.validation_type = 'pitching' THEN vr.accuracy_percentage END) AS pitching_accuracy,
    MAX(CASE WHEN vr.validation_type = 'batting' THEN vr.discrepancies_count END) AS batting_diffs,
    MAX(CASE WHEN vr.validation_type = 'pitching' THEN vr.discrepancies_count END) AS pitching_diffs
FROM validation_reports vr
JOIN games g ON vr.game_id = g.game_id
WHERE vr.accuracy_percentage < 100.0
GROUP BY g.game_id, g.date, g.home_team, g.away_team
ORDER BY g.date DESC, (
    COALESCE(MAX(CASE WHEN vr.validation_type = 'batting' THEN vr.discrepancies_count END), 0) + 
    COALESCE(MAX(CASE WHEN vr.validation_type = 'pitching' THEN vr.discrepancies_count END), 0)
) DESC;
