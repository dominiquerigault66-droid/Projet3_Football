-- models/marts/v_top_valeur_par_poste.sql
-- Benchmark valeur marchande : top joueurs par poste et par ligue
-- Utile pour les visualisations comparatives dans Streamlit

WITH ranked AS (
    SELECT
        joueur_id,
        nom,
        age,
        nationality,
        position,
        position_detail,
        club,
        league,
        photo_url,
        team_logo,
        valeur_eur,
        valeur_texte,
        sfs_rating,
        sfs_goals,
        sfs_assists,
        goals_assists_p90,
        score_sport,
        score_sport_label,
        score_marketing,
        score_marketing_label,
        -- Rang par poste (toutes ligues confondues)
        RANK() OVER (
            PARTITION BY position
            ORDER BY valeur_eur DESC
        ) AS rang_poste,
        -- Rang par poste et par ligue
        RANK() OVER (
            PARTITION BY position, league
            ORDER BY valeur_eur DESC
        ) AS rang_poste_ligue
    FROM {{ ref('v_joueurs_complets') }}
    WHERE valeur_eur IS NOT NULL
      AND position IS NOT NULL
)

SELECT * FROM ranked
