"""
scraper_capology_sofascore.py
Récupère les salaires (Capology) et les stats sportives (Sofascore) par ligue,
puis joint les résultats sur l'ID Transfermarkt via le nom normalisé du joueur.

Robustesse (alignée sur monScraperFC.py) :
  - Retry par appel source  (MAX_RETRIES_SOURCE tentatives par ligue/source)
  - Checkpoint par ligue    (reprise sans tout recommencer en cas d'interruption)
  - Sauvegarde progressive  (une ligue traitée = une ligne ajoutée au CSV)
  - Isolation des erreurs   (une source en erreur ne bloque pas l'autre)

Couverture confirmée par check_capology.py + check_sofascore.py :

  Ligue                      Capology       Sofascore
  ─────────────────────────  ─────────────  ─────────
  Argentina Liga Profesional ❌ absent       ✅ 2024
  Belgium Pro League         ✅ 2024-25      ❌ absent
  Brazil Serie A             ❌ absent       ❌ absent
  England Premier League     ✅ 2024-25      ✅ 24/25
  France Ligue 1             ✅ 2024-25      ✅ 24/25
  Germany Bundesliga         ✅ 2024-25      ✅ 24/25
  Italy Serie A              ✅ 2024-25      ✅ 24/25
  Netherlands Eredivisie     ✅ 2024-25      ✅ 24/25
  Portugal Primeira Liga     ✅ 2024-25      ✅ 24/25
  Scotland Premier League    ❌ absent       ❌ absent
  Spain La Liga              ⚠️ erreur scrp  ✅ 24/25
  Turkiye Super Lig          ⚠️ erreur scrp  ✅ 24/25
  USA MLS                    ❌ absent       ✅ 2024

Sortie : data/transfermarkt/players_enriched.csv
  - ID, Name, league         (clés Transfermarkt)
  - cap_*, capology_name     (colonnes Capology, préfixées)
  - sfs_*, sofascore_name    (colonnes Sofascore, préfixées)
  Les joueurs sans correspondance ont NaN sur les colonnes de la source manquante.
"""

import sys
import io
import time
import random
import json
import logging
import unicodedata
import re
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
from ScraperFC import Capology, Sofascore

# ── Fix encodage Windows ───────────────────────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── Configuration ──────────────────────────────────────────────────────────────
OUTPUT_DIR       = Path(__file__).parent / "data" / "transfermarkt"
DATA_DIR         = Path(__file__).parent / "data"
INPUT_FILE       = DATA_DIR   / "players_all.csv"        # CSV Transfermarkt dans data/
OUTPUT_FILE      = DATA_DIR   / "players_enriched.csv"   # CSV enrichi dans data/
CHECKPOINT_FILE  = OUTPUT_DIR / "checkpoint_enrichment.json"
LOG_FILE         = OUTPUT_DIR / "scraper_enrichment.log"

CAPOLOGY_CURRENCY  = "eur"    # "eur", "gbp" ou "usd"
SOFASCORE_ACCUM    = "total"  # "total", "per90" ou "perMatch"
MAX_RETRIES_SOURCE = 2        # tentatives par (ligue, source)

DELAY_BETWEEN_LEAGUES = (10, 20)  # secondes

LEAGUE_CONFIG = {
    # Nom TM                         Capology                       Sais. Cap.   Sofascore                      Sais. Sfs.
    # ── Capology ✅  Sofascore ✅ ──────────────────────────────────────────────────────────────────────────────────────
    "England Premier League":      ("England Premier League",       "2024-25",   "England Premier League",      "24/25"),
    "France Ligue 1":              ("France Ligue 1",               "2024-25",   "France Ligue 1",              "24/25"),
    "Germany Bundesliga":          ("Germany Bundesliga",           "2024-25",   "Germany Bundesliga",          "24/25"),
    "Italy Serie A":               ("Italy Serie A",                "2024-25",   "Italy Serie A",               "24/25"),
    "Netherlands Eredivisie":      ("Netherlands Eredivisie",       "2024-25",   "Netherlands Eredivisie",      "24/25"),
    "Portugal Primeira Liga":      ("Portugal Primeira Liga",       "2024-25",   "Portugal Primeira Liga",      "24/25"),
    # ── Capology ❌  Sofascore ✅ ──────────────────────────────────────────────────────────────────────────────────────
    "Argentina Liga Profesional":  (None,                           None,        "Argentina Liga Profesional",  "2024"),
    "Spain La Liga":               (None,                           None,        "Spain La Liga",               "24/25"),
    "Turkiye Super Lig":           (None,                           None,        "Turkiye Super Lig",           "24/25"),
    "USA MLS":                     (None,                           None,        "USA MLS",                     "2024"),
    # ── Capology ✅  Sofascore ❌ ──────────────────────────────────────────────────────────────────────────────────────
    "Belgium Pro League":          ("Belgium Pro League",           "2024-25",   None,                          None),
    # ── Capology ❌  Sofascore ❌ ──────────────────────────────────────────────────────────────────────────────────────
    "Brazil Serie A":              (None,                           None,        None,                          None),
    "Scotland Premier League":     (None,                           None,        None,                          None),
}

# ── Logging ────────────────────────────────────────────────────────────────────
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── Checkpoint ─────────────────────────────────────────────────────────────────
def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {
        "done":   [],   # clés "ligue|source" entièrement traitées avec succès
        "failed": [],   # clés "ligue|source" en échec persistant après retries
    }

def save_checkpoint(cp: dict):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f, indent=2)

def append_league_to_csv(df: pd.DataFrame, filepath: Path):
    """
    Ajoute les lignes d'une ligue au CSV.
    Si la ligue est déjà présente dans le fichier, ses lignes sont remplacées
    (évite les doublons en cas de relance après interruption).
    """
    if not filepath.exists():
        df.to_csv(filepath, index=False, encoding="utf-8")
        return

    df_existing = pd.read_csv(filepath, encoding="utf-8")
    league = df["league"].iloc[0]
    # Supprimer les lignes de cette ligue déjà présentes
    df_existing = df_existing[df_existing["league"] != league]
    # Réaligner les colonnes (nouvelles colonnes Capology/Sofascore absentes des ligues précédentes)
    df_combined = pd.concat([df_existing, df], ignore_index=True)
    df_combined.to_csv(filepath, index=False, encoding="utf-8")

# ── Normalisation des noms pour la jointure ────────────────────────────────────
def normalize_name(name: str) -> str:
    """
    Supprime accents, ponctuation et met en minuscules.
    Ex : "João Félix" -> "joaofelix"
    """
    if not isinstance(name, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]", "", ascii_str.lower())

# ── Fetch avec retry ───────────────────────────────────────────────────────────
def fetch_with_retry(scraper, method: str, league: str, year: str, prefix: str, name_candidates: list[str]) -> pd.DataFrame | None:
    """
    Appelle scraper.<method>(year, league, ...) avec MAX_RETRIES_SOURCE tentatives.
    Normalise le nom du joueur et préfixe les colonnes.
    Retourne un DataFrame prêt pour la jointure, ou None si échec.
    """
    for attempt in range(1, MAX_RETRIES_SOURCE + 1):
        try:
            fn = getattr(scraper, method)
            if prefix == "cap":
                df = fn(year, league, CAPOLOGY_CURRENCY)
            else:
                df = fn(year, league, accumulation=SOFASCORE_ACCUM)

            if df is None or df.empty:
                log.warning(f"  {prefix.upper()} : aucune donnée pour {league} {year}")
                return None

            # Aplatir le MultiIndex si nécessaire (Capology retourne des tuples)
            if isinstance(df.columns[0], tuple):
                df.columns = [
                    re.sub(r"\s+", " ", "_".join(str(p) for p in col).strip("_")).strip()
                    for col in df.columns
                ]

            # Identifier la colonne nom
            name_col = next(
                (c for c in df.columns if isinstance(c, str) and c.lower() in name_candidates),
                df.columns[0]
            )
            col_label = "capology_name" if prefix == "cap" else "sofascore_name"
            df = df.rename(columns={name_col: col_label})
            df["name_key"] = df[col_label].apply(normalize_name)

            # Préfixer les colonnes de données
            rename_map = {
                c: f"{prefix}_{c}" for c in df.columns
                if c not in ("name_key", col_label)
            }
            df = df.rename(columns=rename_map)
            log.info(f"  {prefix.upper()} : {len(df)} joueurs récupérés (tentative {attempt})")
            return df

        except Exception as e:
            err = str(e)[:120]
            if attempt < MAX_RETRIES_SOURCE:
                wait = random.uniform(5, 15)
                log.warning(f"  {prefix.upper()} retry {attempt}/{MAX_RETRIES_SOURCE} ({league}) : {err} — pause {wait:.1f}s")
                time.sleep(wait)
            else:
                log.error(f"  {prefix.upper()} échec définitif ({league}) : {err}")
                return None

# ── Pipeline principal ─────────────────────────────────────────────────────────
def enrich_players():
    if not INPUT_FILE.exists():
        log.error(f"Fichier introuvable : {INPUT_FILE}")
        sys.exit(1)

    log.info(f"Chargement de {INPUT_FILE}...")
    df_tm = pd.read_csv(INPUT_FILE, encoding="utf-8")
    log.info(f"{len(df_tm)} joueurs chargés, {df_tm['league'].nunique()} ligues")
    df_tm["name_key"] = df_tm["Name"].apply(normalize_name)

    cp  = load_checkpoint()
    cap = Capology()
    sfs = Sofascore()
    coverage = {}  # ligue -> {cap, sfs}

    for tm_league, (cap_league, cap_year, sfs_league, sfs_year) in LEAGUE_CONFIG.items():
        log.info(f"\n{'='*55}")
        log.info(f"=== {tm_league} ===")
        log.info(f"{'='*55}")

        df_league = df_tm[df_tm["league"] == tm_league].copy()
        if df_league.empty:
            log.warning(f"  Aucun joueur TM pour '{tm_league}', ignoré.")
            continue

        total = len(df_league)
        log.info(f"  {total} joueurs TM dans cette ligue")
        df_out = df_league[["ID", "Name", "league", "name_key"]].copy()
        coverage[tm_league] = {}

        # ── Capology ──────────────────────────────────────────────────────────
        ck_cap = f"{tm_league}|cap"
        if cap_league and ck_cap not in cp["done"]:
            df_cap = fetch_with_retry(
                cap, "scrape_salaries", cap_league, cap_year,
                prefix="cap", name_candidates=["player", "name", "player name"]
            )
            if df_cap is not None:
                df_out = df_out.merge(df_cap, on="name_key", how="left")
                matched = df_out["capology_name"].notna().sum()
                log.info(f"  Jointure Capology : {matched}/{total} ({matched/total*100:.1f}%)")
                coverage[tm_league]["cap"] = (matched, total)
                cp["done"].append(ck_cap)
                if ck_cap in cp["failed"]:
                    cp["failed"].remove(ck_cap)
            else:
                coverage[tm_league]["cap"] = (0, total)
                if ck_cap not in cp["failed"]:
                    cp["failed"].append(ck_cap)
        elif ck_cap in cp["done"]:
            log.info(f"  Capology : déjà traité [checkpoint]")
            coverage[tm_league]["cap"] = ("checkpoint", total)
        else:
            log.info(f"  Capology : non disponible pour cette ligue")
            coverage[tm_league]["cap"] = None

        # ── Sofascore ─────────────────────────────────────────────────────────
        ck_sfs = f"{tm_league}|sfs"
        if sfs_league and ck_sfs not in cp["done"]:
            df_sfs = fetch_with_retry(
                sfs, "scrape_player_league_stats", sfs_league, sfs_year,
                prefix="sfs", name_candidates=["player", "name", "player name"]
            )
            if df_sfs is not None:
                df_out = df_out.merge(df_sfs, on="name_key", how="left")
                matched = df_out["sofascore_name"].notna().sum()
                log.info(f"  Jointure Sofascore : {matched}/{total} ({matched/total*100:.1f}%)")
                coverage[tm_league]["sfs"] = (matched, total)
                cp["done"].append(ck_sfs)
                if ck_sfs in cp["failed"]:
                    cp["failed"].remove(ck_sfs)
            else:
                coverage[tm_league]["sfs"] = (0, total)
                if ck_sfs not in cp["failed"]:
                    cp["failed"].append(ck_sfs)
        elif ck_sfs in cp["done"]:
            log.info(f"  Sofascore : déjà traité [checkpoint]")
            coverage[tm_league]["sfs"] = ("checkpoint", total)
        else:
            log.info(f"  Sofascore : non disponible pour cette ligue")
            coverage[tm_league]["sfs"] = None

        # ── Sauvegarde progressive + checkpoint ───────────────────────────────
        df_out["scraped_at"] = datetime.now(timezone.utc).isoformat()
        df_out = df_out.drop(columns=["name_key"], errors="ignore")
        append_league_to_csv(df_out, OUTPUT_FILE)
        save_checkpoint(cp)
        log.info(f"  Ligue sauvegardée dans {OUTPUT_FILE}")

        wait = random.uniform(*DELAY_BETWEEN_LEAGUES)
        log.info(f"  Pause {wait:.1f}s...")
        time.sleep(wait)

    # ── Rapport final ──────────────────────────────────────────────────────────
    log.info(f"\n{'='*65}")
    if OUTPUT_FILE.exists():
        df_final = pd.read_csv(OUTPUT_FILE, encoding="utf-8")
        log.info(f"Fichier final : {OUTPUT_FILE}")
        log.info(f"Total : {len(df_final)} joueurs, {len(df_final.columns)} colonnes")
    log.info(f"\n{'Ligue':<35} {'Capology':^20} {'Sofascore':^20}")
    log.info(f"{'-'*35} {'-'*20} {'-'*20}")

    def fmt(val):
        if val is None:             return "N/A (absent)"
        if val[0] == "checkpoint":  return f"[reprise] /{val[1]}"
        matched, total = val
        return f"{matched}/{total} ({matched/total*100:.0f}%)" if total else "0/0"

    for league, s in coverage.items():
        log.info(f"{league:<35} {fmt(s.get('cap')):^20} {fmt(s.get('sfs')):^20}")

    if cp["failed"]:
        log.warning(f"\nSources en échec persistant : {cp['failed']}")

    return pd.read_csv(OUTPUT_FILE, encoding="utf-8") if OUTPUT_FILE.exists() else pd.DataFrame()


if __name__ == "__main__":
    df = enrich_players()
    if not df.empty:
        cap_cols = [c for c in df.columns if c.startswith("cap_")]
        sfs_cols = [c for c in df.columns if c.startswith("sfs_")]
        print(f"\nColonnes Capology  ({len(cap_cols)}) : {cap_cols}")
        print(f"Colonnes Sofascore ({len(sfs_cols)}) : {sfs_cols}")
        print(f"\nAperçu :")
        print(df.head())