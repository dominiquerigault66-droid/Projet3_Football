-- ============================================================
--  PROJET DATA ANALYST — FOOTBALL
--  Schéma OLAP en étoile — MySQL
--  WildCodeSchool 11/25 - 04/26  |  Équipe : Carla, Patricia, Dominique
-- ============================================================
--  Génération : depuis recap_joueurs_clean.csv (16 946 joueurs · 210 col.)
--  Sources intégrées : API-Football, ESPN, Transfermarkt, Sofascore,
--                      TheSportsDB, Capology, FBref
-- ============================================================

SET NAMES utf8mb4;
SET time_zone = '+00:00';

CREATE DATABASE IF NOT EXISTS football_db
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE football_db;

-- ============================================================
-- TABLE CENTRALE : joueurs
-- Identité de base + IDs sources pour jointures aval
-- ============================================================
CREATE TABLE IF NOT EXISTS joueurs (
    joueur_id       INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,

    -- Nom unifié (priorité : espn > tm > tsdb > apife > apif > fbref)
    nom             VARCHAR(150)    NOT NULL,

    -- Colonnes consolidées (calculées dans Nettoyage_joueurs.py)
    birth_date      DATE,
    nationality     VARCHAR(100),
    position        ENUM('Attacker','Midfielder','Defender','Goalkeeper'),
    position_detail VARCHAR(100),
    club            VARCHAR(150),
    league          VARCHAR(100),

    -- IDs sources (pour traçabilité et jointures potentielles)
    apif_id         INT UNSIGNED,
    apife_player_id INT UNSIGNED,
    espn_id         INT UNSIGNED,
    tm_id           INT UNSIGNED,
    enr_id          INT UNSIGNED,
    tsdb_id         INT UNSIGNED,

    -- Photo principale (apif en priorité, tsdb en fallback)
    photo_url       VARCHAR(512),

    INDEX idx_position  (position),
    INDEX idx_club      (club),
    INDEX idx_league    (league),
    INDEX idx_nationality (nationality),
    INDEX idx_birth_date  (birth_date),
    INDEX idx_tm_id     (tm_id),
    INDEX idx_tsdb_id   (tsdb_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================
-- TABLE : profil
-- Données physiques et visuelles du joueur
-- Sources : TheSportsDB, API-Football, Transfermarkt
-- ============================================================
CREATE TABLE IF NOT EXISTS profil (
    profil_id       INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    joueur_id       INT UNSIGNED NOT NULL,

    height_cm       FLOAT,                      -- taille en cm (tsdb > tm > espn > apif)
    weight_kg       FLOAT,                      -- poids en kg  (apif > tsdb > espn)
    pied_fort       VARCHAR(20),                -- tsdb_strSide : Right / Left / Both
    apife_number    TINYINT UNSIGNED,           -- numéro de maillot (apife_player_number)
    espn_number     TINYINT UNSIGNED,           -- numéro de maillot ESPN (espn_jersey_number)

    -- Photos / visuels
    apif_photo      VARCHAR(512),               -- photo API-Football
    apife_photo     VARCHAR(512),               -- photo apife (effectifs)
    espn_photo      VARCHAR(512),               -- photo ESPN
    tsdb_thumb      VARCHAR(512),               -- photo TheSportsDB (avec fond)
    tsdb_cutout     VARCHAR(512),               -- photo TheSportsDB (sans fond, détouré)
    apife_team_logo VARCHAR(512),               -- logo du club (apife_team_logo)
    fbref_url       VARCHAR(512),               -- URL fiche FBref

    FOREIGN KEY (joueur_id) REFERENCES joueurs(joueur_id) ON DELETE CASCADE,
    UNIQUE KEY uq_profil_joueur (joueur_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================
-- TABLE : performances
-- Statistiques de performance saison — Sofascore (principal) + ESPN (fallback)
-- Couverture Sofascore : ~5 489 joueurs (32%) | ESPN : ~3 700 (22%)
-- ============================================================
CREATE TABLE IF NOT EXISTS performances (
    perf_id                         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    joueur_id                       INT UNSIGNED NOT NULL,
    stats_source                    ENUM('sofascore','espn') NOT NULL DEFAULT 'sofascore',

    -- ── Sofascore (enr_sfs_*) ──────────────────────────────────────────────
    -- Généraux
    sfs_rating                      FLOAT,      -- note moyenne Sofascore
    sfs_appearances                 SMALLINT,
    sfs_matches_started             SMALLINT,
    sfs_minutes_played              INT,
    sfs_totw_appearances            TINYINT,    -- sélections équipe de la semaine

    -- Attaque
    sfs_goals                       SMALLINT,
    sfs_assists                     SMALLINT,
    sfs_goals_assists_sum           SMALLINT,
    sfs_expected_goals              FLOAT,
    sfs_expected_assists            FLOAT,
    sfs_shots_on_target             SMALLINT,
    sfs_total_shots                 SMALLINT,
    sfs_big_chances_missed          SMALLINT,
    sfs_big_chances_created         SMALLINT,
    sfs_goal_conversion_pct         FLOAT,
    sfs_scoring_frequency           FLOAT,
    sfs_goals_from_inside_box       SMALLINT,
    sfs_goals_from_outside_box      SMALLINT,
    sfs_headed_goals                SMALLINT,
    sfs_left_foot_goals             SMALLINT,
    sfs_right_foot_goals            SMALLINT,
    sfs_free_kick_goal              SMALLINT,
    sfs_penalty_goals               SMALLINT,
    sfs_penalties_taken             SMALLINT,
    sfs_penalty_conversion          FLOAT,
    sfs_hit_woodwork                SMALLINT,
    sfs_offsides                    SMALLINT,

    -- Passes
    sfs_key_passes                  SMALLINT,
    sfs_accurate_passes             SMALLINT,
    sfs_total_passes                SMALLINT,
    sfs_accurate_passes_pct         FLOAT,
    sfs_accurate_long_balls         SMALLINT,
    sfs_accurate_long_balls_pct     FLOAT,
    sfs_accurate_crosses            SMALLINT,
    sfs_total_cross                 SMALLINT,
    sfs_accurate_crosses_pct        FLOAT,
    sfs_accurate_final_third        SMALLINT,
    sfs_pass_to_assist              SMALLINT,
    sfs_total_attempt_assist        SMALLINT,

    -- Défense
    sfs_tackles                     SMALLINT,
    sfs_tackles_won                 SMALLINT,
    sfs_tackles_won_pct             FLOAT,
    sfs_interceptions               SMALLINT,
    sfs_clearances                  SMALLINT,
    sfs_blocked_shots               SMALLINT,
    sfs_outfielder_blocks           SMALLINT,
    sfs_aerial_duels_won            SMALLINT,
    sfs_aerial_duels_won_pct        FLOAT,
    sfs_aerial_lost                 SMALLINT,
    sfs_ground_duels_won            SMALLINT,
    sfs_ground_duels_won_pct        FLOAT,
    sfs_total_duels_won             SMALLINT,
    sfs_total_duels_won_pct         FLOAT,
    sfs_duel_lost                   SMALLINT,
    sfs_error_lead_to_goal          SMALLINT,
    sfs_error_lead_to_shot          SMALLINT,
    sfs_ball_recovery               SMALLINT,
    sfs_possession_won_att_third    SMALLINT,

    -- Gardien
    sfs_saves                       SMALLINT,
    sfs_saves_caught                SMALLINT,
    sfs_saves_parried               SMALLINT,
    sfs_saved_from_inside_box       SMALLINT,
    sfs_saved_from_outside_box      SMALLINT,
    sfs_goals_conceded              SMALLINT,
    sfs_goals_conceded_inside_box   SMALLINT,
    sfs_goals_conceded_outside_box  SMALLINT,
    sfs_goals_prevented             FLOAT,
    sfs_clean_sheet                 SMALLINT,
    sfs_penalty_save                SMALLINT,
    sfs_penalty_faced               SMALLINT,
    sfs_high_claims                 SMALLINT,
    sfs_punches                     SMALLINT,
    sfs_runs_out                    SMALLINT,
    sfs_successful_runs_out         SMALLINT,
    sfs_crosses_not_claimed         SMALLINT,
    sfs_goal_kicks                  SMALLINT,

    -- Dribbles / duels
    sfs_successful_dribbles         SMALLINT,
    sfs_successful_dribbles_pct     FLOAT,
    sfs_dribbled_past               SMALLINT,
    sfs_total_contest               SMALLINT,
    sfs_dispossessed                SMALLINT,
    sfs_touches                     INT,
    sfs_possession_lost             SMALLINT,
    sfs_own_goals                   SMALLINT,
    sfs_was_fouled                  SMALLINT,
    sfs_fouls                       SMALLINT,
    sfs_yellow_cards                SMALLINT,
    sfs_yellow_red_cards            SMALLINT,
    sfs_red_cards                   SMALLINT,
    sfs_direct_red_cards            SMALLINT,
    sfs_penalty_conceded            SMALLINT,
    sfs_penalty_won                 SMALLINT,
    sfs_set_piece_conversion        FLOAT,
    sfs_shot_from_set_piece         SMALLINT,

    -- ── ESPN fallback (espn_*) ─────────────────────────────────────────────
    espn_goals                      SMALLINT,
    espn_assists                    SMALLINT,
    espn_appearances                SMALLINT,
    espn_minutes_played             INT,
    espn_yellow_cards               SMALLINT,
    espn_red_cards                  SMALLINT,
    espn_shots                      SMALLINT,
    espn_shots_on_target            SMALLINT,
    espn_tackles                    SMALLINT,
    espn_fouls                      SMALLINT,
    espn_passes                     SMALLINT,
    espn_pass_accuracy              FLOAT,
    espn_saves                      SMALLINT,
    espn_clean_sheets               SMALLINT,
    espn_rating                     FLOAT,

    FOREIGN KEY (joueur_id) REFERENCES joueurs(joueur_id) ON DELETE CASCADE,
    UNIQUE KEY uq_perf_joueur (joueur_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================
-- TABLE : clubs
-- Club actuel et informations de ligue
-- Sources : API-Football Équipes, ESPN, Transfermarkt
-- ============================================================
CREATE TABLE IF NOT EXISTS clubs (
    club_id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    joueur_id       INT UNSIGNED NOT NULL,

    team_name       VARCHAR(150),               -- club (colonne consolidée)
    league_name     VARCHAR(100),               -- league (colonne consolidée)
    apife_team_id   INT UNSIGNED,               -- ID équipe API-Football
    team_logo       VARCHAR(512),               -- logo équipe (apife_team_logo)
    player_number   TINYINT UNSIGNED,           -- numéro de maillot (apife > espn)

    FOREIGN KEY (joueur_id) REFERENCES joueurs(joueur_id) ON DELETE CASCADE,
    UNIQUE KEY uq_club_joueur (joueur_id),
    INDEX idx_team_name  (team_name),
    INDEX idx_league     (league_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================
-- TABLE : valeur_marchande
-- Valorisation financière — Transfermarkt
-- Couverture : tm_Value ~51%, tm_Contract ~55%
-- ============================================================
CREATE TABLE IF NOT EXISTS valeur_marchande (
    vm_id                   INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    joueur_id               INT UNSIGNED NOT NULL,

    -- Valeur actuelle (stockée en euros — parsée depuis '€12.00m' / '€700k')
    valeur_eur              BIGINT,             -- tm_Value parsé en entier (€)
    valeur_texte            VARCHAR(20),        -- tm_Value brut original ('€12.00m')
    valeur_last_updated     VARCHAR(50),        -- tm_Value last updated (texte libre)
    valeur_history_json     TEXT,               -- tm_Market value history (JSON brut)

    -- Transfert
    last_club               VARCHAR(150),       -- tm_Last club
    joined_date             VARCHAR(50),        -- tm_Joined (texte libre)
    since_date              VARCHAR(50),        -- tm_Since (texte libre)
    transfer_history_json   TEXT,               -- tm_Transfer history (JSON brut)
    tm_player_url           VARCHAR(512),       -- URL fiche Transfermarkt

    FOREIGN KEY (joueur_id) REFERENCES joueurs(joueur_id) ON DELETE CASCADE,
    UNIQUE KEY uq_vm_joueur (joueur_id),
    INDEX idx_valeur_eur    (valeur_eur)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================
-- TABLE : contrats
-- Données salariales et contractuelles — Capology + Transfermarkt
-- Couverture : Capology ~19%, tm_Contract ~55%
-- ============================================================
CREATE TABLE IF NOT EXISTS contrats (
    contrat_id              INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    joueur_id               INT UNSIGNED NOT NULL,

    -- Transfermarkt
    contrat_expiration      VARCHAR(50),        -- tm_Contract expiration (texte : 'Jun 30, 2026')
    contrat_expiration_date DATE,               -- parsé en DATE pour filtres Streamlit

    -- Capology (salaires estimés)
    salaire_brut_semaine_eur    DECIMAL(12,2),  -- enr_cap_EST. BASE SALARY_GROSS P/W (EUR)
    salaire_brut_annuel_eur     DECIMAL(14,2),  -- enr_cap_EST. BASE SALARY_GROSS P/Y (EUR)
    salaire_adj_annuel_eur      DECIMAL(14,2),  -- enr_cap_EST. BASE SALARY_ADJ. GROSS (EUR)
    cap_position                VARCHAR(50),    -- enr_cap_BIO_POS.
    cap_club                    VARCHAR(150),   -- enr_cap_CLUB

    FOREIGN KEY (joueur_id) REFERENCES joueurs(joueur_id) ON DELETE CASCADE,
    UNIQUE KEY uq_contrat_joueur (joueur_id),
    INDEX idx_expiration (contrat_expiration_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================
-- TABLE : notoriete
-- Présence digitale et notoriété — TheSportsDB
-- Couverture : intLoved ~60%, Instagram ~10%, Description EN ~47%
-- ============================================================
CREATE TABLE IF NOT EXISTS notoriete (
    notoriete_id            INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    joueur_id               INT UNSIGNED NOT NULL,

    -- Réseaux sociaux (stockés comme URL ou handle)
    instagram               VARCHAR(255),       -- tsdb_strInstagram
    twitter                 VARCHAR(255),       -- tsdb_strTwitter
    facebook                VARCHAR(255),       -- tsdb_strFacebook

    -- Popularité TheSportsDB (0-15, très concentré : Messi=15)
    int_loved               TINYINT UNSIGNED,   -- tsdb_intLoved

    -- Description biographique
    description_en          TEXT,               -- tsdb_strDescriptionEN (~47%)
    description_fr          TEXT,               -- tsdb_strDescriptionFR (~0.3% — fallback)

    -- Informations complémentaires TheSportsDB
    tsdb_gender             VARCHAR(10),        -- tsdb_strGender
    tsdb_signing            VARCHAR(100),       -- tsdb_strSigning (agent / agence)
    tsdb_wage               VARCHAR(50),        -- tsdb_strWage (texte libre)

    FOREIGN KEY (joueur_id) REFERENCES joueurs(joueur_id) ON DELETE CASCADE,
    UNIQUE KEY uq_notoriete_joueur (joueur_id),
    INDEX idx_int_loved (int_loved)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
