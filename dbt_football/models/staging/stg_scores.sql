-- models/staging/stg_scores.sql
-- Calcul des composantes brutes des scores sportif et marketing
-- avant transformation en percentiles (effectuée dans marts/v_joueurs_complets)

WITH joueurs AS (
    SELECT * FROM {{ ref('stg_joueurs') }}
),

perfs AS (
    SELECT * FROM {{ ref('stg_performances') }}
),

notoriete AS (
    SELECT
        joueur_id,
        int_loved,
        instagram,
        twitter,
        facebook
    FROM football_db.notoriete
),

valeurs AS (
    SELECT
        joueur_id,
        valeur_eur
    FROM football_db.valeur_marchande
),

-- ── Composantes score sportif ──────────────────────────────────────────────────
score_sport_raw AS (
    SELECT
        j.joueur_id,
        j.position,
        j.age,
        p.sfs_rating,
        p.goals_assists_p90,
        p.saves_p90,
        p.sfs_clean_sheet,
        p.sfs_minutes_played,

        -- Composante rating (commune à tous les postes) — normalisée 0-1
        -- sfs_rating est entre ~5 et ~10 → on normalise sur cet intervalle
        LEAST(GREATEST((COALESCE(p.sfs_rating, 0) - 5) / 5, 0), 1)
            AS comp_rating,

        -- Composante buts+assists/90 pondérée par poste
        CASE j.position
            WHEN 'Goalkeeper' THEN
                -- Gardien : saves/90 + clean_sheets normalisés
                LEAST(COALESCE(p.saves_p90, 0) / 5, 1) * 0.6
                + LEAST(COALESCE(p.sfs_clean_sheet, 0) / 20, 1) * 0.4
            WHEN 'Defender' THEN
                LEAST(COALESCE(p.goals_assists_p90, 0) / 0.5, 1)
            WHEN 'Midfielder' THEN
                LEAST(COALESCE(p.goals_assists_p90, 0) / 0.8, 1)
            WHEN 'Attacker' THEN
                LEAST(COALESCE(p.goals_assists_p90, 0) / 1.2, 1)
            ELSE
                LEAST(COALESCE(p.goals_assists_p90, 0) / 0.8, 1)
        END AS comp_contribution,

        -- Composante régularité : minutes jouées normalisées (max ~3000 min/saison)
        LEAST(COALESCE(p.sfs_minutes_played, 0) / 3000, 1)
            AS comp_regularite,

        -- Composante âge inversé : bonus jeunesse (< 23 ans = 1, > 33 ans = 0)
        CASE
            WHEN j.age IS NULL THEN 0.5
            WHEN j.age <= 23    THEN 1.0
            WHEN j.age >= 33    THEN 0.0
            ELSE (33 - j.age) / 10.0
        END AS comp_age

    FROM joueurs j
    LEFT JOIN perfs p ON j.joueur_id = p.joueur_id
),

-- ── Score sportif brut pondéré par poste ──────────────────────────────────────
score_sport_weighted AS (
    SELECT
        joueur_id,
        position,
        comp_rating,
        comp_contribution,
        comp_regularite,
        comp_age,
        -- Pondération selon poste
        CASE position
            WHEN 'Goalkeeper' THEN
                comp_rating      * 0.35
                + comp_contribution * 0.30
                + comp_regularite   * 0.20
                + comp_age          * 0.15
            WHEN 'Defender' THEN
                comp_rating      * 0.55
                + comp_contribution * 0.10
                + comp_regularite   * 0.20
                + comp_age          * 0.15
            WHEN 'Midfielder' THEN
                comp_rating      * 0.45
                + comp_contribution * 0.20
                + comp_regularite   * 0.20
                + comp_age          * 0.15
            WHEN 'Attacker' THEN
                comp_rating      * 0.35
                + comp_contribution * 0.30
                + comp_regularite   * 0.20
                + comp_age          * 0.15
            ELSE
                comp_rating      * 0.45
                + comp_contribution * 0.20
                + comp_regularite   * 0.20
                + comp_age          * 0.15
        END AS score_sport_brut,

        -- Flag : score calculable (joueur avec données Sofascore)
        CASE WHEN comp_rating > 0 OR comp_contribution > 0 THEN 1 ELSE 0 END
            AS has_sport_data

    FROM score_sport_raw
),

-- ── Composantes score marketing ───────────────────────────────────────────────
score_marketing_raw AS (
    SELECT
        j.joueur_id,
        j.league,
        n.int_loved,

        -- int_loved déconcentré via log (valeurs 0-15 → log(1-16))
        LOG(COALESCE(n.int_loved, 0) + 1) / LOG(16)
            AS comp_loved,

        -- Réseaux sociaux : nombre de plateformes renseignées / 3
        (
            CASE WHEN n.instagram IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN n.twitter  IS NOT NULL THEN 1 ELSE 0 END
            + CASE WHEN n.facebook IS NOT NULL THEN 1 ELSE 0 END
        ) / 3.0 AS comp_reseaux,

        -- Ligue premium (top 5 européennes + Champions League)
        CASE
            WHEN j.league IN (
                'Premier League', 'La Liga', 'Serie A',
                'Bundesliga', 'Ligue 1', 'Champions League'
            ) THEN 1.0
            ELSE 0.0
        END AS comp_ligue

    FROM joueurs j
    LEFT JOIN notoriete n ON j.joueur_id = n.joueur_id
),

-- ── Score marketing brut ──────────────────────────────────────────────────────
score_marketing_weighted AS (
    SELECT
        joueur_id,
        comp_loved,
        comp_reseaux,
        comp_ligue,
        -- Pondération : int_loved 55%, réseaux 30%, ligue 15%
        comp_loved   * 0.55
        + comp_reseaux * 0.30
        + comp_ligue   * 0.15 AS score_marketing_brut
    FROM score_marketing_raw
)

-- ── Assemblage final ──────────────────────────────────────────────────────────
SELECT
    sw.joueur_id,
    sw.position,
    sw.score_sport_brut,
    sw.has_sport_data,
    sw.comp_rating,
    sw.comp_contribution,
    sw.comp_regularite,
    sw.comp_age,
    mw.score_marketing_brut,
    mw.comp_loved,
    mw.comp_reseaux,
    mw.comp_ligue
FROM score_sport_weighted sw
LEFT JOIN score_marketing_weighted mw ON sw.joueur_id = mw.joueur_id
