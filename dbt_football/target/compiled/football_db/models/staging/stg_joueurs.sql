-- models/staging/stg_joueurs.sql
-- Staging : table joueurs — sélection et typage des colonnes utiles

WITH source AS (
    SELECT
        joueur_id,
        nom,
        birth_date,
        nationality,
        position,
        position_detail,
        club,
        league,
        photo_url,
        apif_id,
        apife_player_id,
        espn_id,
        tm_id,
        tsdb_id,
        -- Âge calculé à la date du jour
        TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) AS age
    FROM football_db.joueurs
    WHERE nom IS NOT NULL
)

SELECT * FROM source