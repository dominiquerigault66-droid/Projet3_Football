-- models/staging/stg_performances.sql
-- Staging : performances — calcul des métriques par 90 minutes

WITH source AS (
    SELECT
        joueur_id,
        stats_source,
        -- Sofascore — métriques brutes
        sfs_rating,
        sfs_appearances,
        sfs_minutes_played,
        sfs_goals,
        sfs_assists,
        sfs_saves,
        sfs_clean_sheet,
        sfs_key_passes,
        sfs_successful_dribbles,
        sfs_tackles,
        sfs_interceptions,
        sfs_yellow_cards,
        sfs_red_cards,
        sfs_expected_goals,
        sfs_expected_assists,
        -- Métriques /90 min (évite division par zéro)
        CASE
            WHEN sfs_minutes_played > 0
            THEN ROUND((sfs_goals + sfs_assists) / sfs_minutes_played * 90, 3)
            ELSE NULL
        END AS goals_assists_p90,
        CASE
            WHEN sfs_minutes_played > 0
            THEN ROUND(sfs_goals / sfs_minutes_played * 90, 3)
            ELSE NULL
        END AS goals_p90,
        CASE
            WHEN sfs_minutes_played > 0
            THEN ROUND(sfs_assists / sfs_minutes_played * 90, 3)
            ELSE NULL
        END AS assists_p90,
        CASE
            WHEN sfs_minutes_played > 0
            THEN ROUND(sfs_saves / sfs_minutes_played * 90, 3)
            ELSE NULL
        END AS saves_p90,
        CASE
            WHEN sfs_minutes_played > 0
            THEN ROUND(sfs_key_passes / sfs_minutes_played * 90, 3)
            ELSE NULL
        END AS key_passes_p90,
        -- ESPN fallback (colonnes moins riches)
        espn_goals,
        espn_assists,
        espn_appearances,
        espn_minutes_played,
        espn_rating
    FROM football_db.performances
    WHERE sfs_minutes_played > 0
       OR espn_minutes_played > 0
)

SELECT * FROM source