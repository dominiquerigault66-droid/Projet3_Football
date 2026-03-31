"""
Pipeline Transfermarkt — v5
- scrape_players() remplace par get_player_links() + scrape_player() en boucle
- Chaque joueur est scrappe individuellement -> un joueur bugue n'arrete pas la ligue
- Sauvegarde joueur par joueur (aucune perte en cas d'interruption)
- [v5] Aplatissement des colonnes multi-lignes avant ecriture CSV
        (Other positions, Market value history, Transfer history)
"""

import sys
import io
import time
import random
import json
import logging
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

# ── Fix encodage Windows CP1252 -> UTF-8 ──────────────────────────────────────
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ── Fix timeout botasaurus (AVANT import ScraperFC) ───────────────────────────
try:
    import botasaurus_requests as br
    br.DEFAULT_TIMEOUT = 60
except Exception:
    pass

from ScraperFC import Transfermarkt

# ── Configuration ─────────────────────────────────────────────────────────────
OUTPUT_DIR      = Path(__file__).parent / "data" / "transfermarkt"
DATA_DIR        = Path(__file__).parent / "data"
CHECKPOINT_FILE = OUTPUT_DIR / "checkpoint.json"
OUTPUT_FILE     = DATA_DIR   / "players_all.csv"      # CSV final dans data/
ERRORS_FILE     = OUTPUT_DIR / "players_errors.csv"   # fichier intermédiaire dans data/transfermarkt/

DELAY_BETWEEN_PLAYERS = (3, 8)    # secondes entre chaque joueur
DELAY_BETWEEN_LEAGUES = (15, 30)  # secondes entre chaque ligue
MAX_RETRIES_PLAYER = 2            # tentatives par joueur

# Saison par ligue (format different selon la ligue)
# Ligues calendaires (Amerique du Sud, MLS) : "2024"
# Ligues europeennes                         : "24/25"
LEAGUE_SEASONS = {
    "Argentina Liga Profesional": "2024",
    "Belgium Pro League":         "24/25",
    "Brazil Serie A":             "2024",
    "England Premier League":     "24/25",
    "France Ligue 1":             "24/25",
    "Germany Bundesliga":         "24/25",
    "Italy Serie A":              "24/25",
    "Netherlands Eredivisie":     "24/25",
    "Portugal Primeira Liga":     "24/25",
    "Scotland Premier League":    "24/25",
    "Spain La Liga":              "24/25",
    "Turkiye Super Lig":          "24/25",
    "USA MLS":                    "2024",
}
LEAGUES = list(LEAGUE_SEASONS.keys())

# ── Setup ──────────────────────────────────────────────────────────────────────
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(OUTPUT_DIR / "scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
)
log = logging.getLogger(__name__)

# ── Aplatissement des colonnes problematiques ─────────────────────────────────
def _parse_df_column(val) -> list[str]:
    """
    Convertit un DataFrame serialise en texte (ou un vrai DataFrame)
    en liste de valeurs. Retourne [] si vide ou invalide.
    Exemple d'entree (texte) :
        "               0
        0    Left Winger
        1  Left Midfield"
    Exemple de sortie : ["Left Winger", "Left Midfield"]
    """
    if val is None:
        return []
    # Si c'est deja un vrai DataFrame pandas
    if isinstance(val, pd.DataFrame):
        if val.empty:
            return []
        # Prend la premiere colonne de valeurs
        return [str(v).strip() for v in val.iloc[:, 0].tolist() if str(v).strip()]
    # Si c'est une chaine de texte (DataFrame serialise)
    text = str(val).strip()
    if not text or "Empty DataFrame" in text:
        return []
    lines = text.splitlines()
    result = []
    for line in lines:
        parts = line.strip().split(None, 1)  # split sur premier espace
        if len(parts) == 2:
            try:
                int(parts[0])           # verifie que le premier token est un index
                result.append(parts[1].strip())
            except ValueError:
                pass                    # ligne d'en-tete ou autre -> ignore
    return result


def _parse_transfer_history(val) -> list[dict]:
    """
    Convertit l'historique des transferts (DataFrame ou texte) en liste de dicts.
    Retourne [] si vide.
    """
    if val is None:
        return []
    if isinstance(val, pd.DataFrame):
        if val.empty:
            return []
        return val.to_dict(orient="records")
    text = str(val).strip()
    if not text or "Empty DataFrame" in text:
        return []
    # Tenter de reconstruire le DataFrame depuis le texte
    try:
        from io import StringIO
        df = pd.read_fwf(StringIO(text))
        if df.empty:
            return []
        return df.to_dict(orient="records")
    except Exception:
        return []


def _parse_mv_history(val) -> list[dict]:
    """
    Convertit l'historique de valeur marchande en liste de dicts.
    Retourne [] si vide.
    """
    return _parse_transfer_history(val)   # meme logique


def flatten_row(row: dict) -> dict:
    """
    Aplatie les colonnes multi-lignes d'un dict joueur :
      - 'Other positions'       -> JSON list  ex. '["Left Winger", "Centre-Forward"]'
      - 'Market value history'  -> JSON list  ex. '[{"Season":"24/25","MV":"€14m",...}]'
      - 'Transfer history'      -> JSON list  ex. '[{"Season":"...","Fee":"..."}]'
    Toutes les autres colonnes sont laissees intactes.
    """
    flat = dict(row)

    if "Other positions" in flat:
        positions = _parse_df_column(flat["Other positions"])
        flat["Other positions"] = json.dumps(positions, ensure_ascii=False)

    if "Market value history" in flat:
        mv = _parse_mv_history(flat["Market value history"])
        flat["Market value history"] = json.dumps(mv, ensure_ascii=False)

    if "Transfer history" in flat:
        th = _parse_transfer_history(flat["Transfer history"])
        flat["Transfer history"] = json.dumps(th, ensure_ascii=False)

    return flat

# ── Checkpoint (granularite : joueur individuel) ───────────────────────────────
def load_checkpoint():
    if CHECKPOINT_FILE.exists():
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {
        "done_leagues": [],       # ligues entierement terminees
        "failed_leagues": [],     # ligues en echec total
        "done_players": [],       # URLs de joueurs deja scrappe
        "failed_players": [],     # URLs de joueurs en erreur persistante
    }

def save_checkpoint(cp):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(cp, f, indent=2)

def append_row_to_csv(row: dict, filepath: Path):
    """Ecrit une ligne dans le CSV (cree le fichier si necessaire)."""
    df = pd.DataFrame([row])
    write_header = not filepath.exists()
    df.to_csv(filepath, mode="a", index=False, header=write_header, encoding="utf-8")

# ── Scraping d'un joueur avec retry ───────────────────────────────────────────
def scrape_one_player(tm: Transfermarkt, player_url: str, league: str) -> dict | None:
    """
    Scrappe un joueur individuel. Retourne un dict aplati ou None si echec.
    """
    for attempt in range(1, MAX_RETRIES_PLAYER + 1):
        try:
            df = tm.scrape_player(player_url)
            if df is None or df.empty:
                return None
            row = df.iloc[0].to_dict()
            row["player_url"] = player_url
            row["league"] = league
            row["scraped_at"] = datetime.now(timezone.utc).isoformat()
            return flatten_row(row)   # <-- [v5] aplatissement ici
        except Exception as e:
            err = str(e)[:120]
            if attempt < MAX_RETRIES_PLAYER:
                log.info(f"    Retry joueur {player_url[-20:]} (tentative {attempt}) : {err}")
                time.sleep(random.uniform(5, 10))
            else:
                log.warning(f"    Joueur ignore : {player_url} -> {err}")
                return None

# ── Scraping d'une ligue ───────────────────────────────────────────────────────
def scrape_league(league: str, cp: dict) -> int:
    """
    Scrappe tous les joueurs d'une ligue un par un.
    Retourne le nombre de joueurs scrappe avec succes.
    """
    tm = Transfermarkt()
    count_ok = 0
    count_err = 0

    # Etape 1 : recuperer tous les liens joueurs de la ligue
    log.info(f"  Recuperation des liens joueurs...")
    try:
        player_links = tm.get_player_links(LEAGUE_SEASONS[league], league)
        log.info(f"  {len(player_links)} joueurs trouves dans {league}")
    except Exception as e:
        log.error(f"  Impossible de recuperer les liens : {e}")
        return 0

    total = len(player_links)

    # Etape 2 : scraper chaque joueur individuellement
    for i, url in enumerate(player_links, 1):
        # Skip si deja fait (reprise apres interruption)
        if url in cp["done_players"]:
            count_ok += 1
            continue

        if i % 10 == 0 or i == 1:
            log.info(f"  Progression : {i}/{total} ({count_ok} OK, {count_err} erreurs)")

        row = scrape_one_player(tm, url, league)

        if row:
            append_row_to_csv(row, OUTPUT_FILE)
            cp["done_players"].append(url)
            count_ok += 1
        else:
            cp["failed_players"].append(url)
            count_err += 1
            append_row_to_csv({
                "player_url": url,
                "league": league,
                "error": "scrape_failed",
                "scraped_at": datetime.now(timezone.utc).isoformat()
            }, ERRORS_FILE)

        # Sauvegarde checkpoint toutes les 10 joueurs
        if i % 10 == 0:
            save_checkpoint(cp)

        time.sleep(random.uniform(*DELAY_BETWEEN_PLAYERS))

    save_checkpoint(cp)
    log.info(f"  Ligue terminee : {count_ok} OK, {count_err} erreurs")
    return count_ok

# ── Pipeline principal ────────────────────────────────────────────────────────
def scrape_all():
    cp = load_checkpoint()

    for league in LEAGUES:
        if league in cp["done_leagues"]:
            log.info(f"[SKIP] {league} deja traite")
            continue

        log.info(f"\n{'='*55}")
        log.info(f"=== Ligue : {league} ===")
        log.info(f"{'='*55}")

        n = scrape_league(league, cp)

        if n > 0:
            cp["done_leagues"].append(league)
            if league in cp["failed_leagues"]:
                cp["failed_leagues"].remove(league)
        else:
            if league not in cp["failed_leagues"]:
                cp["failed_leagues"].append(league)

        save_checkpoint(cp)

        wait = random.uniform(*DELAY_BETWEEN_LEAGUES)
        log.info(f"  Pause {wait:.1f}s avant la ligue suivante...")
        time.sleep(wait)

    # Rapport final
    log.info(f"\n{'='*55}")
    log.info(f"Ligues terminees : {len(cp['done_leagues'])}/{len(LEAGUES)}")
    if cp["failed_leagues"]:
        log.warning(f"Ligues echouees : {cp['failed_leagues']}")
    if cp["failed_players"]:
        log.warning(f"Joueurs en erreur : {len(cp['failed_players'])} (voir {ERRORS_FILE})")

    # Chargement du resultat final
    if OUTPUT_FILE.exists():
        df = pd.read_csv(OUTPUT_FILE, encoding="utf-8")
        log.info(f"Total : {len(df)} joueurs dans {OUTPUT_FILE}")
        log.info(f"Colonnes : {list(df.columns)}")
        return df
    return pd.DataFrame()


if __name__ == "__main__":
    df = scrape_all()
    if not df.empty:
        print(f"\nApercu :")
        print(df.head())