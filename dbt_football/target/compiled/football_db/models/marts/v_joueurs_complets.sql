-- models/marts/v_joueurs_complets.sql
-- Vue centrale dénormalisée — base de toutes les requêtes Streamlit
-- Contient les scores S1-S10 (sportif) et M1-M10 (marketing) en percentiles

WITH joueurs AS (
    SELECT * FROM `football_db`.`stg_joueurs`
),

perfs AS (
    SELECT * FROM `football_db`.`stg_performances`
),

scores_raw AS (
    SELECT * FROM `football_db`.`stg_scores`
),

profil AS (
    SELECT
        joueur_id,
        height_cm,
        weight_kg,
        pied_fort,
        apif_photo,
        apife_photo,
        espn_photo,
        tsdb_thumb,
        tsdb_cutout,
        apife_team_logo,
        fbref_url
    FROM football_db.profil
),

clubs AS (
    SELECT
        joueur_id,
        team_name,
        league_name,
        apife_team_id,
        team_logo,
        player_number
    FROM football_db.clubs
),

valeur AS (
    SELECT
        joueur_id,
        valeur_eur,
        valeur_texte,
        last_club,
        joined_date,
        tm_player_url
    FROM football_db.valeur_marchande
),

contrats AS (
    SELECT
        joueur_id,
        contrat_expiration,
        contrat_expiration_date,
        salaire_brut_semaine_eur,
        salaire_brut_annuel_eur,
        cap_position,
        cap_club
    FROM football_db.contrats
),

notoriete AS (
    SELECT
        joueur_id,
        instagram,
        twitter,
        facebook,
        int_loved,
        description_en,
        description_fr,
        tsdb_gender,
        tsdb_signing,
        tsdb_wage
    FROM football_db.notoriete
),

-- ── Percentiles scores sportifs (joueurs avec données uniquement) ─────────────
sport_percentiles AS (
    SELECT
        joueur_id,
        score_sport_brut,
        has_sport_data,
        -- Rang percentile 0-1 parmi les joueurs avec données, par poste
        PERCENT_RANK() OVER (
            PARTITION BY position
            ORDER BY score_sport_brut
        ) AS pct_sport
    FROM scores_raw
    WHERE has_sport_data = 1
),

-- ── NTILE scores marketing (tous joueurs) — distribution uniforme ─────────────
marketing_percentiles AS (
    SELECT
        joueur_id,
        score_marketing_brut,
        NTILE(10) OVER (
            ORDER BY score_marketing_brut
        ) AS ntile_marketing
    FROM scores_raw
),

-- ── Conversion percentiles → échelle S1-S10 / M1-M10 ─────────────────────────
scores_finaux AS (
    SELECT
        sp.joueur_id,
        -- Score sportif : NULL si pas de données Sofascore
        CASE
            WHEN sp.pct_sport IS NOT NULL
            THEN LEAST(FLOOR(sp.pct_sport * 10) + 1, 10)
            ELSE NULL
        END AS score_sport,
        CASE
            WHEN sp.pct_sport IS NOT NULL
            THEN CONCAT('S', LEAST(FLOOR(sp.pct_sport * 10) + 1, 10))
            ELSE NULL
        END AS score_sport_label,
        -- Score marketing : tous joueurs
        mp.ntile_marketing AS score_marketing,
        CONCAT('M', mp.ntile_marketing) AS score_marketing_label
    FROM sport_percentiles sp
    LEFT JOIN marketing_percentiles mp ON sp.joueur_id = mp.joueur_id

    UNION ALL

    -- Joueurs sans données sportives : score_sport NULL, marketing calculé
    SELECT
        mp.joueur_id,
        NULL AS score_sport,
        NULL AS score_sport_label,
        mp.ntile_marketing AS score_marketing,
        CONCAT('M', mp.ntile_marketing) AS score_marketing_label
    FROM marketing_percentiles mp
    WHERE mp.joueur_id NOT IN (SELECT joueur_id FROM sport_percentiles)
)

-- ── Assemblage final ──────────────────────────────────────────────────────────
SELECT
    -- Identité
    j.joueur_id,
    j.nom,
    j.birth_date,
    j.age,
    j.nationality,
    j.position,
    j.position_detail,
    j.club,
    j.league,

    -- Photo principale (priorité : apif > tsdb_cutout > espn > tsdb_thumb)
    COALESCE(pr.apif_photo, pr.tsdb_cutout, pr.espn_photo, pr.tsdb_thumb)
        AS photo_url,

    -- Profil physique
    pr.height_cm,
    pr.weight_kg,
    pr.pied_fort,
    pr.apife_team_logo AS team_logo,
    pr.fbref_url,

    -- Club
    cl.player_number,

    -- Valeur marchande
    v.valeur_eur,
    v.valeur_texte,
    v.tm_player_url,

    -- Contrat
    ct.contrat_expiration,
    ct.contrat_expiration_date,
    ct.salaire_brut_semaine_eur,
    ct.salaire_brut_annuel_eur,
    ct.cap_position,

    -- Stats performance clés
    p.sfs_rating,
    p.sfs_appearances,
    p.sfs_minutes_played,
    p.sfs_goals,
    p.sfs_assists,
    p.goals_assists_p90,
    p.goals_p90,
    p.assists_p90,
    p.saves_p90,
    p.sfs_expected_goals,
    p.sfs_expected_assists,
    p.sfs_yellow_cards,
    p.sfs_red_cards,

    -- Notoriété
    n.instagram,
    n.twitter,
    n.facebook,
    n.int_loved,
    n.description_en,
    n.description_fr,
    n.tsdb_signing,

    -- Scores finaux
    sf.score_sport,
    sf.score_sport_label,
    sf.score_marketing,
    sf.score_marketing_label,

    -- IDs sources (traçabilité)
    j.apif_id,
    j.apife_player_id,
    j.espn_id,
    j.tm_id,
    j.tsdb_id

FROM joueurs j
LEFT JOIN profil pr       ON j.joueur_id = pr.joueur_id
LEFT JOIN clubs cl        ON j.joueur_id = cl.joueur_id
LEFT JOIN valeur v        ON j.joueur_id = v.joueur_id
LEFT JOIN contrats ct     ON j.joueur_id = ct.joueur_id
LEFT JOIN perfs p         ON j.joueur_id = p.joueur_id
LEFT JOIN notoriete n     ON j.joueur_id = n.joueur_id
LEFT JOIN scores_finaux sf ON j.joueur_id = sf.joueur_id