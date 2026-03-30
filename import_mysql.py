"""
import_mysql.py
==============
Importe recap_joueurs_clean.csv dans la base MySQL football_db.

Usage :
    python import_mysql.py

Prérequis :
    pip install sqlalchemy pymysql pandas python-dotenv
    Fichier .env à la racine du projet (voir README_db.md)
    Base initialisée : mysql -u root -p < init_db.sql

Ordre d'insertion :
    1. joueurs   (table centrale — génère les joueur_id)
    2. profil
    3. clubs
    4. valeur_marchande
    5. contrats
    6. performances
    7. notoriete
"""

import re
import ast
import warnings
from pathlib import Path

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

warnings.filterwarnings("ignore")

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()

DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "football_db")

DATA_DIR = Path(__file__).parent / "data"
CSV_PATH = DATA_DIR / "recap_joueurs_clean.csv"

CHUNK_SIZE = 1000  # lignes insérées par batch

# ── Connexion ─────────────────────────────────────────────────────────────────
engine = create_engine(
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    "?charset=utf8mb4",
    echo=False,
)

# ── Helpers ───────────────────────────────────────────────────────────────────

def _na(val):
    """Convertit NaN/NaT/None en None (compatible SQL NULL)."""
    if val is None:
        return None
    if isinstance(val, float) and np.isnan(val):
        return None
    return val


def parse_tm_value(s) -> int | None:
    """
    '€12.00m'  → 12_000_000
    '€700k'    → 700_000
    '€1.50m'   → 1_500_000
    """
    if pd.isna(s):
        return None
    s = str(s).strip().replace(",", ".")
    m = re.match(r"[€$]?([\d.]+)\s*m", s, re.I)
    if m:
        return int(float(m.group(1)) * 1_000_000)
    m = re.match(r"[€$]?([\d.]+)\s*k", s, re.I)
    if m:
        return int(float(m.group(1)) * 1_000)
    try:
        return int(float(re.sub(r"[^\d.]", "", s)))
    except (ValueError, TypeError):
        return None


def parse_contract_date(s) -> str | None:
    """
    'Jun 30, 2026' → '2026-06-30'
    Retourne None si non parsable.
    """
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(str(s), format="%b %d, %Y").strftime("%Y-%m-%d")
    except Exception:
        return None


def coalesce(*args):
    """Retourne la première valeur non-None/NaN de la liste."""
    for a in args:
        if a is not None and not (isinstance(a, float) and np.isnan(a)):
            return a
    return None


def safe_int(v) -> int | None:
    try:
        f = float(v)
        return None if np.isnan(f) else int(f)
    except (ValueError, TypeError):
        return None


def safe_float(v) -> float | None:
    try:
        f = float(v)
        return None if np.isnan(f) else round(f, 4)
    except (ValueError, TypeError):
        return None


def safe_str(v, maxlen=None) -> str | None:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    s = str(v).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return None
    return s[:maxlen] if maxlen else s


# ── Chargement ────────────────────────────────────────────────────────────────
print(f"Chargement de {CSV_PATH} …")
df = pd.read_csv(CSV_PATH, low_memory=False)
print(f"  {len(df)} lignes × {len(df.columns)} colonnes")

# ── Nom unifié ────────────────────────────────────────────────────────────────
# Priorité : espn_name > tm_Name > tsdb_strPlayer > apife_player_name > apif_name > fbref_Nom
name_cols = ["espn_name", "tm_Name", "tsdb_strPlayer",
             "apife_player_name", "apif_name", "fbref_Nom"]
df["_nom"] = df[[c for c in name_cols if c in df.columns]].bfill(axis=1).iloc[:, 0]

# ── Photo principale ──────────────────────────────────────────────────────────
df["_photo"] = (
    df.get("apif_photo", pd.Series(np.nan, index=df.index))
    .combine_first(df.get("tsdb_strThumb", pd.Series(np.nan, index=df.index)))
    .combine_first(df.get("espn_photo", pd.Series(np.nan, index=df.index)))
)

# ── Insertion ─────────────────────────────────────────────────────────────────

def insert_chunks(conn, table: str, records: list[dict], label: str):
    """Insère en ignorant les doublons (INSERT IGNORE)."""
    if not records:
        print(f"  [{label}] 0 lignes — rien à insérer")
        return
    cols = list(records[0].keys())
    placeholders = ", ".join(f":{c}" for c in cols)
    col_list = ", ".join(f"`{c}`" for c in cols)
    sql = text(
        f"INSERT IGNORE INTO `{table}` ({col_list}) VALUES ({placeholders})"
    )
    total = 0
    for i in range(0, len(records), CHUNK_SIZE):
        chunk = records[i : i + CHUNK_SIZE]
        conn.execute(sql, chunk)
        total += len(chunk)
    print(f"  [{label}] {total} lignes insérées dans `{table}`")


with engine.begin() as conn:

    # ── 1. joueurs ────────────────────────────────────────────────────────────
    print("\n[1/7] Table joueurs …")
    joueurs_rows = []
    for _, r in df.iterrows():
        nom = safe_str(r.get("_nom"), 150)
        if not nom:
            continue  # ligne sans nom ignorée
        joueurs_rows.append({
            "nom":              nom,
            "birth_date":       safe_str(r.get("birth_date")),
            "nationality":      safe_str(r.get("nationality"), 100),
            "position":         safe_str(r.get("position")),
            "position_detail":  safe_str(r.get("position_detail"), 100),
            "club":             safe_str(r.get("club"), 150),
            "league":           safe_str(r.get("league"), 100),
            "apif_id":          safe_int(r.get("apif_id")),
            "apife_player_id":  safe_int(r.get("apife_player_id")),
            "espn_id":          safe_int(r.get("espn_id")),
            "tm_id":            safe_int(r.get("tm_ID")),
            "enr_id":           safe_int(r.get("enr_ID")),
            "tsdb_id":          safe_int(r.get("tsdb_idPlayer")),
            "photo_url":        safe_str(r.get("_photo"), 512),
        })
    insert_chunks(conn, "joueurs", joueurs_rows, "joueurs")

    # Récupération de la map nom → joueur_id pour les tables filles
    result = conn.execute(text(
        "SELECT joueur_id, nom, tm_id, tsdb_id, espn_id FROM joueurs"
    ))
    id_map: dict[int, int] = {}   # index df (position) → joueur_id MySQL
    # On mappe par (nom, tm_id) pour maximiser la précision
    joueur_lookup: list[tuple] = [
        (row.joueur_id, row.nom, row.tm_id, row.tsdb_id, row.espn_id)
        for row in result
    ]

    # Index rapide : nom → joueur_id (premier match suffit car IGNORE)
    nom_to_jid: dict[str, int] = {}
    for jid, nom, *_ in joueur_lookup:
        if nom not in nom_to_jid:
            nom_to_jid[nom] = jid

    def get_jid(r) -> int | None:
        nom = safe_str(r.get("_nom"), 150)
        return nom_to_jid.get(nom)

    # ── 2. profil ─────────────────────────────────────────────────────────────
    print("[2/7] Table profil …")
    profil_rows = []
    for _, r in df.iterrows():
        jid = get_jid(r)
        if jid is None:
            continue
        profil_rows.append({
            "joueur_id":       jid,
            "height_cm":       safe_float(r.get("height_cm")),
            "weight_kg":       safe_float(r.get("weight_kg")),
            "pied_fort":       safe_str(r.get("tsdb_strSide"), 20),
            "apife_number":    safe_int(r.get("apife_player_number")),
            "espn_number":     safe_int(r.get("espn_jersey_number")),
            "apif_photo":      safe_str(r.get("apif_photo"), 512),
            "apife_photo":     safe_str(r.get("apife_player_photo"), 512),
            "espn_photo":      safe_str(r.get("espn_photo"), 512),
            "tsdb_thumb":      safe_str(r.get("tsdb_strThumb"), 512),
            "tsdb_cutout":     safe_str(r.get("tsdb_strCutout"), 512),
            "apife_team_logo": safe_str(r.get("apife_team_logo"), 512),
            "fbref_url":       safe_str(r.get("fbref_url"), 512),
        })
    insert_chunks(conn, "profil", profil_rows, "profil")

    # ── 3. clubs ──────────────────────────────────────────────────────────────
    print("[3/7] Table clubs …")
    clubs_rows = []
    for _, r in df.iterrows():
        jid = get_jid(r)
        if jid is None:
            continue
        # numéro de maillot : apife en priorité, sinon espn
        number = coalesce(
            safe_int(r.get("apife_player_number")),
            safe_int(r.get("espn_jersey_number")),
        )
        clubs_rows.append({
            "joueur_id":      jid,
            "team_name":      safe_str(r.get("club"), 150),
            "league_name":    safe_str(r.get("league"), 100),
            "apife_team_id":  safe_int(r.get("apife_team_id")),
            "team_logo":      safe_str(r.get("apife_team_logo"), 512),
            "player_number":  number,
        })
    insert_chunks(conn, "clubs", clubs_rows, "clubs")

    # ── 4. valeur_marchande ───────────────────────────────────────────────────
    print("[4/7] Table valeur_marchande …")
    vm_rows = []
    for _, r in df.iterrows():
        jid = get_jid(r)
        if jid is None:
            continue
        valeur_texte = safe_str(r.get("tm_Value"), 20)
        vm_rows.append({
            "joueur_id":            jid,
            "valeur_eur":           parse_tm_value(r.get("tm_Value")),
            "valeur_texte":         valeur_texte,
            "valeur_last_updated":  safe_str(r.get("tm_Value last updated"), 50),
            "valeur_history_json":  safe_str(r.get("tm_Market value history")),
            "last_club":            safe_str(r.get("tm_Last club"), 150),
            "joined_date":          safe_str(r.get("tm_Joined"), 50),
            "since_date":           safe_str(r.get("tm_Since"), 50),
            "transfer_history_json": safe_str(r.get("tm_Transfer history")),
            "tm_player_url":        safe_str(r.get("tm_player_url"), 512),
        })
    insert_chunks(conn, "valeur_marchande", vm_rows, "valeur_marchande")

    # ── 5. contrats ───────────────────────────────────────────────────────────
    print("[5/7] Table contrats …")
    contrats_rows = []
    for _, r in df.iterrows():
        jid = get_jid(r)
        if jid is None:
            continue
        exp_texte = safe_str(r.get("tm_Contract expiration"), 50)
        contrats_rows.append({
            "joueur_id":                jid,
            "contrat_expiration":       exp_texte,
            "contrat_expiration_date":  parse_contract_date(exp_texte),
            "salaire_brut_semaine_eur": safe_float(r.get("enr_cap_EST. BASE SALARY_GROSS P/W (EUR)")),
            "salaire_brut_annuel_eur":  safe_float(r.get("enr_cap_EST. BASE SALARY_GROSS P/Y (EUR)")),
            "salaire_adj_annuel_eur":   safe_float(r.get("enr_cap_EST. BASE SALARY_ADJ. GROSS (EUR)")),
            "cap_position":             safe_str(r.get("enr_cap_BIO_POS."), 50),
            "cap_club":                 safe_str(r.get("enr_cap_CLUB"), 150),
        })
    insert_chunks(conn, "contrats", contrats_rows, "contrats")

    # ── 6. performances ───────────────────────────────────────────────────────
    print("[6/7] Table performances …")
    perf_rows = []
    for _, r in df.iterrows():
        jid = get_jid(r)
        if jid is None:
            continue
        has_sfs = pd.notna(r.get("enr_sfs_rating"))
        has_espn = pd.notna(r.get("espn_goals"))
        if not has_sfs and not has_espn:
            continue  # pas de stats du tout → on n'insère pas
        source = "sofascore" if has_sfs else "espn"
        perf_rows.append({
            "joueur_id":    jid,
            "stats_source": source,
            # Sofascore
            "sfs_rating":                   safe_float(r.get("enr_sfs_rating")),
            "sfs_appearances":              safe_int(r.get("enr_sfs_appearances")),
            "sfs_matches_started":          safe_int(r.get("enr_sfs_matchesStarted")),
            "sfs_minutes_played":           safe_int(r.get("enr_sfs_minutesPlayed")),
            "sfs_totw_appearances":         safe_int(r.get("enr_sfs_totwAppearances")),
            "sfs_goals":                    safe_int(r.get("enr_sfs_goals")),
            "sfs_assists":                  safe_int(r.get("enr_sfs_assists")),
            "sfs_goals_assists_sum":        safe_int(r.get("enr_sfs_goalsAssistsSum")),
            "sfs_expected_goals":           safe_float(r.get("enr_sfs_expectedGoals")),
            "sfs_expected_assists":         safe_float(r.get("enr_sfs_expectedAssists")),
            "sfs_shots_on_target":          safe_int(r.get("enr_sfs_shotsOnTarget")),
            "sfs_total_shots":              safe_int(r.get("enr_sfs_totalShots")),
            "sfs_big_chances_missed":       safe_int(r.get("enr_sfs_bigChancesMissed")),
            "sfs_big_chances_created":      safe_int(r.get("enr_sfs_bigChancesCreated")),
            "sfs_goal_conversion_pct":      safe_float(r.get("enr_sfs_goalConversionPercentage")),
            "sfs_scoring_frequency":        safe_float(r.get("enr_sfs_scoringFrequency")),
            "sfs_goals_from_inside_box":    safe_int(r.get("enr_sfs_goalsFromInsideTheBox")),
            "sfs_goals_from_outside_box":   safe_int(r.get("enr_sfs_goalsFromOutsideTheBox")),
            "sfs_headed_goals":             safe_int(r.get("enr_sfs_headedGoals")),
            "sfs_left_foot_goals":          safe_int(r.get("enr_sfs_leftFootGoals")),
            "sfs_right_foot_goals":         safe_int(r.get("enr_sfs_rightFootGoals")),
            "sfs_free_kick_goal":           safe_int(r.get("enr_sfs_freeKickGoal")),
            "sfs_penalty_goals":            safe_int(r.get("enr_sfs_penaltyGoals")),
            "sfs_penalties_taken":          safe_int(r.get("enr_sfs_penaltiesTaken")),
            "sfs_penalty_conversion":       safe_float(r.get("enr_sfs_penaltyConversion")),
            "sfs_hit_woodwork":             safe_int(r.get("enr_sfs_hitWoodwork")),
            "sfs_offsides":                 safe_int(r.get("enr_sfs_offsides")),
            "sfs_key_passes":               safe_int(r.get("enr_sfs_keyPasses")),
            "sfs_accurate_passes":          safe_int(r.get("enr_sfs_accuratePasses")),
            "sfs_total_passes":             safe_int(r.get("enr_sfs_totalPasses")),
            "sfs_accurate_passes_pct":      safe_float(r.get("enr_sfs_accuratePassesPercentage")),
            "sfs_accurate_long_balls":      safe_int(r.get("enr_sfs_accurateLongBalls")),
            "sfs_accurate_long_balls_pct":  safe_float(r.get("enr_sfs_accurateLongBallsPercentage")),
            "sfs_accurate_crosses":         safe_int(r.get("enr_sfs_accurateCrosses")),
            "sfs_total_cross":              safe_int(r.get("enr_sfs_totalCross")),
            "sfs_accurate_crosses_pct":     safe_float(r.get("enr_sfs_accurateCrossesPercentage")),
            "sfs_accurate_final_third":     safe_int(r.get("enr_sfs_accurateFinalThirdPasses")),
            "sfs_pass_to_assist":           safe_int(r.get("enr_sfs_passToAssist")),
            "sfs_total_attempt_assist":     safe_int(r.get("enr_sfs_totalAttemptAssist")),
            "sfs_tackles":                  safe_int(r.get("enr_sfs_tackles")),
            "sfs_tackles_won":              safe_int(r.get("enr_sfs_tacklesWon")),
            "sfs_tackles_won_pct":          safe_float(r.get("enr_sfs_tacklesWonPercentage")),
            "sfs_interceptions":            safe_int(r.get("enr_sfs_interceptions")),
            "sfs_clearances":               safe_int(r.get("enr_sfs_clearances")),
            "sfs_blocked_shots":            safe_int(r.get("enr_sfs_blockedShots")),
            "sfs_outfielder_blocks":        safe_int(r.get("enr_sfs_outfielderBlocks")),
            "sfs_aerial_duels_won":         safe_int(r.get("enr_sfs_aerialDuelsWon")),
            "sfs_aerial_duels_won_pct":     safe_float(r.get("enr_sfs_aerialDuelsWonPercentage")),
            "sfs_aerial_lost":              safe_int(r.get("enr_sfs_aerialLost")),
            "sfs_ground_duels_won":         safe_int(r.get("enr_sfs_groundDuelsWon")),
            "sfs_ground_duels_won_pct":     safe_float(r.get("enr_sfs_groundDuelsWonPercentage")),
            "sfs_total_duels_won":          safe_int(r.get("enr_sfs_totalDuelsWon")),
            "sfs_total_duels_won_pct":      safe_float(r.get("enr_sfs_totalDuelsWonPercentage")),
            "sfs_duel_lost":                safe_int(r.get("enr_sfs_duelLost")),
            "sfs_error_lead_to_goal":       safe_int(r.get("enr_sfs_errorLeadToGoal")),
            "sfs_error_lead_to_shot":       safe_int(r.get("enr_sfs_errorLeadToShot")),
            "sfs_ball_recovery":            safe_int(r.get("enr_sfs_ballRecovery")),
            "sfs_possession_won_att_third": safe_int(r.get("enr_sfs_possessionWonAttThird")),
            "sfs_saves":                    safe_int(r.get("enr_sfs_saves")),
            "sfs_saves_caught":             safe_int(r.get("enr_sfs_savesCaught")),
            "sfs_saves_parried":            safe_int(r.get("enr_sfs_savesParried")),
            "sfs_saved_from_inside_box":    safe_int(r.get("enr_sfs_savedShotsFromInsideTheBox")),
            "sfs_saved_from_outside_box":   safe_int(r.get("enr_sfs_savedShotsFromOutsideTheBox")),
            "sfs_goals_conceded":           safe_int(r.get("enr_sfs_goalsConceded")),
            "sfs_goals_conceded_inside_box": safe_int(r.get("enr_sfs_goalsConcededInsideTheBox")),
            "sfs_goals_conceded_outside_box": safe_int(r.get("enr_sfs_goalsConcededOutsideTheBox")),
            "sfs_goals_prevented":          safe_float(r.get("enr_sfs_goalsPrevented")),
            "sfs_clean_sheet":              safe_int(r.get("enr_sfs_cleanSheet")),
            "sfs_penalty_save":             safe_int(r.get("enr_sfs_penaltySave")),
            "sfs_penalty_faced":            safe_int(r.get("enr_sfs_penaltyFaced")),
            "sfs_high_claims":              safe_int(r.get("enr_sfs_highClaims")),
            "sfs_punches":                  safe_int(r.get("enr_sfs_punches")),
            "sfs_runs_out":                 safe_int(r.get("enr_sfs_runsOut")),
            "sfs_successful_runs_out":      safe_int(r.get("enr_sfs_successfulRunsOut")),
            "sfs_crosses_not_claimed":      safe_int(r.get("enr_sfs_crossesNotClaimed")),
            "sfs_goal_kicks":               safe_int(r.get("enr_sfs_goalKicks")),
            "sfs_successful_dribbles":      safe_int(r.get("enr_sfs_successfulDribbles")),
            "sfs_successful_dribbles_pct":  safe_float(r.get("enr_sfs_successfulDribblesPercentage")),
            "sfs_dribbled_past":            safe_int(r.get("enr_sfs_dribbledPast")),
            "sfs_total_contest":            safe_int(r.get("enr_sfs_totalContest")),
            "sfs_dispossessed":             safe_int(r.get("enr_sfs_dispossessed")),
            "sfs_touches":                  safe_int(r.get("enr_sfs_touches")),
            "sfs_possession_lost":          safe_int(r.get("enr_sfs_possessionLost")),
            "sfs_own_goals":                safe_int(r.get("enr_sfs_ownGoals")),
            "sfs_was_fouled":               safe_int(r.get("enr_sfs_wasFouled")),
            "sfs_fouls":                    safe_int(r.get("enr_sfs_fouls")),
            "sfs_yellow_cards":             safe_int(r.get("enr_sfs_yellowCards")),
            "sfs_yellow_red_cards":         safe_int(r.get("enr_sfs_yellowRedCards")),
            "sfs_red_cards":                safe_int(r.get("enr_sfs_redCards")),
            "sfs_direct_red_cards":         safe_int(r.get("enr_sfs_directRedCards")),
            "sfs_penalty_conceded":         safe_int(r.get("enr_sfs_penaltyConceded")),
            "sfs_penalty_won":              safe_int(r.get("enr_sfs_penaltyWon")),
            "sfs_set_piece_conversion":     safe_float(r.get("enr_sfs_setPieceConversion")),
            "sfs_shot_from_set_piece":      safe_int(r.get("enr_sfs_shotFromSetPiece")),
            # ESPN fallback
            "espn_goals":           safe_int(r.get("espn_goals")),
            "espn_assists":         safe_int(r.get("espn_assists")),
            "espn_appearances":     safe_int(r.get("espn_appearances")),
            "espn_minutes_played":  safe_int(r.get("espn_minutes_played")),
            "espn_yellow_cards":    safe_int(r.get("espn_yellow_cards")),
            "espn_red_cards":       safe_int(r.get("espn_red_cards")),
            "espn_shots":           safe_int(r.get("espn_shots")),
            "espn_shots_on_target": safe_int(r.get("espn_shots_on_target")),
            "espn_tackles":         safe_int(r.get("espn_tackles")),
            "espn_fouls":           safe_int(r.get("espn_fouls")),
            "espn_passes":          safe_int(r.get("espn_passes")),
            "espn_pass_accuracy":   safe_float(r.get("espn_pass_accuracy")),
            "espn_saves":           safe_int(r.get("espn_saves")),
            "espn_clean_sheets":    safe_int(r.get("espn_clean_sheets")),
            "espn_rating":          safe_float(r.get("espn_rating")),
        })
    insert_chunks(conn, "performances", perf_rows, "performances")

    # ── 7. notoriete ──────────────────────────────────────────────────────────
    print("[7/7] Table notoriete …")
    notoriete_rows = []
    for _, r in df.iterrows():
        jid = get_jid(r)
        if jid is None:
            continue
        notoriete_rows.append({
            "joueur_id":      jid,
            "instagram":      safe_str(r.get("tsdb_strInstagram"), 255),
            "twitter":        safe_str(r.get("tsdb_strTwitter"), 255),
            "facebook":       safe_str(r.get("tsdb_strFacebook"), 255),
            "int_loved":      safe_int(r.get("tsdb_intLoved")),
            "description_en": safe_str(r.get("tsdb_strDescriptionEN")),
            "description_fr": safe_str(r.get("tsdb_strDescriptionFR")),
            "tsdb_gender":    safe_str(r.get("tsdb_strGender"), 10),
            "tsdb_signing":   safe_str(r.get("tsdb_strSigning"), 100),
            "tsdb_wage":      safe_str(r.get("tsdb_strWage"), 50),
        })
    insert_chunks(conn, "notoriete", notoriete_rows, "notoriete")

print("\n✅ Import terminé.")

# ── Rapport final ─────────────────────────────────────────────────────────────
with engine.connect() as conn:
    print("\n── Bilan des tables ──")
    for table in ["joueurs", "profil", "clubs", "valeur_marchande",
                  "contrats", "performances", "notoriete"]:
        n = conn.execute(text(f"SELECT COUNT(*) FROM `{table}`")).scalar()
        print(f"  {table:<20} {n:>7} lignes")
