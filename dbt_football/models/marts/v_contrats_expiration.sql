-- models/marts/v_contrats_expiration.sql
-- Opportunités mercato : joueurs en fin de contrat (< 18 mois)
-- Utile pour identifier des joueurs libres ou en fin de contrat

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

    -- Contrat
    contrat_expiration,
    contrat_expiration_date,
    -- Mois restants avant expiration
    TIMESTAMPDIFF(MONTH, CURDATE(), contrat_expiration_date)
        AS mois_avant_expiration,
    salaire_brut_annuel_eur,

    -- Valeur marchande
    valeur_eur,
    valeur_texte,

    -- Performance
    sfs_rating,
    sfs_goals,
    sfs_assists,
    goals_assists_p90,
    sfs_minutes_played,

    -- Scores
    score_sport,
    score_sport_label,
    score_marketing,
    score_marketing_label

FROM {{ ref('v_joueurs_complets') }}
WHERE
    contrat_expiration_date IS NOT NULL
    AND contrat_expiration_date > CURDATE()
    AND contrat_expiration_date <= DATE_ADD(CURDATE(), INTERVAL 18 MONTH)
ORDER BY contrat_expiration_date ASC
