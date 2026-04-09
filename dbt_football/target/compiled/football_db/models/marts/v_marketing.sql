-- models/marts/v_marketing.sql
-- Vue profil annonceur — notoriété, réseaux sociaux, image
-- Exclut les stats de performance détaillées

SELECT
    joueur_id,
    nom,
    age,
    nationality,
    position,
    club,
    league,
    photo_url,
    team_logo,

    -- Notoriété
    int_loved,
    instagram,
    twitter,
    facebook,
    description_en,
    description_fr,
    tsdb_signing,

    -- Valeur marchande (indicateur de notoriété commerciale)
    valeur_eur,
    valeur_texte,

    -- Stats résumées (suffisent pour le contexte marketing)
    sfs_rating,
    sfs_goals,
    sfs_assists,

    -- Scores
    score_marketing,
    score_marketing_label,
    score_sport,
    score_sport_label

FROM `football_db`.`v_joueurs_complets`