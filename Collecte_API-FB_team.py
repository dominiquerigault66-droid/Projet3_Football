import http.client
import json
import time
import os
import pandas as pd
from dotenv import load_dotenv

# ── Config ────────────────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")
HEADERS = {"x-apisports-key": API_KEY}

LEAGUES       = [140, 128, 61, 39, 71, 94, 88, 200, 144, 78,
                 210, 403, 135, 713, 253, 262, 1212, 207, 98, 290]
SEASONS       = [2023, 2024]   # ← à ajuster selon les saisons disponibles
DELAY         = 6      # secondes entre requêtes (10 req/min)
MAX_REQUESTS  = 100    # quota journalier

# Fichiers de sortie et de progression
import pathlib
DATA_DIR      = pathlib.Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
TEAMS_CSV     = DATA_DIR / "API_F_Clubs.csv"
SQUADS_CSV    = DATA_DIR / "API_F_Equipes.csv"
PROGRESS_FILE = DATA_DIR / "squads_progress.json"

requests_used = 0

# ── Fonction générique d'appel API ────────────────────────────────────────────
def api_get(path: str) -> dict | None:
    global requests_used
    if requests_used >= MAX_REQUESTS:
        print("⛔ Quota journalier atteint.")
        return None
    try:
        conn = http.client.HTTPSConnection("v3.football.api-sports.io")
        conn.request("GET", path, headers=HEADERS)
        res = conn.getresponse()
        requests_used += 1
        print(f"  GET {path} → {res.status} | quota restant : {MAX_REQUESTS - requests_used}")
        if res.status != 200:
            return None
        return json.loads(res.read().decode("utf-8"))
    except Exception as e:
        print(f"  ✗ Erreur : {e}")
        return None
    finally:
        conn.close()

# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 1 — Teams par league + season (ignorée si teams.csv existe déjà)
# ══════════════════════════════════════════════════════════════════════════════
if os.path.isfile(TEAMS_CSV):
    print(f"📂 '{TEAMS_CSV}' trouvé — chargement sans appel API.")
    df_teams_unique = pd.read_csv(TEAMS_CSV)
else:
    teams_rows = []

    for season in SEASONS:
        for league_id in LEAGUES:

            if requests_used >= MAX_REQUESTS:
                print("⛔ Quota atteint pendant la collecte des teams.")
                break

            time.sleep(DELAY)
            data = api_get(f"/teams?league={league_id}&season={season}")
            if data is None:
                continue

            for entry in data.get("response", []):
                t = entry.get("team", {})
                v = entry.get("venue", {})
                teams_rows.append({
                    "league_id":      league_id,
                    "season":         season,
                    "team_id":        t.get("id"),
                    "team_name":      t.get("name"),
                    "team_code":      t.get("code"),
                    "team_country":   t.get("country"),
                    "team_founded":   t.get("founded"),
                    "team_national":  t.get("national"),
                    "team_logo":      t.get("logo"),
                    "venue_id":       v.get("id"),
                    "venue_name":     v.get("name"),
                    "venue_address":  v.get("address"),
                    "venue_city":     v.get("city"),
                    "venue_capacity": v.get("capacity"),
                    "venue_surface":  v.get("surface"),
                    "venue_image":    v.get("image"),
                })

    df_teams_unique = (pd.DataFrame(teams_rows)
                       .drop_duplicates(subset=["team_id", "league_id", "season"]))

    if df_teams_unique.empty:
        print("⚠ Aucune team collectée — vérifiez les leagues_ids et saisons disponibles.")
        exit(0)

    df_teams_unique.to_csv(TEAMS_CSV, index=False, encoding="utf-8")
    print(f"✔ df_teams : {len(df_teams_unique)} lignes sauvegardées dans '{TEAMS_CSV}'")

# ══════════════════════════════════════════════════════════════════════════════
# ÉTAPE 2 — Squads avec reprise automatique
# ══════════════════════════════════════════════════════════════════════════════

# Chargement de la progression existante
if os.path.isfile(PROGRESS_FILE):
    with open(PROGRESS_FILE) as f:
        done_ids = set(json.load(f))
    print(f"📂 Reprise : {len(done_ids)} team(s) déjà traitée(s).")
else:
    done_ids = set()

# Chargement des squads déjà collectés
if os.path.isfile(SQUADS_CSV) and done_ids:
    df_squads_existing = pd.read_csv(SQUADS_CSV)
    squads_rows = df_squads_existing.to_dict("records")
    print(f"📂 '{SQUADS_CSV}' chargé ({len(squads_rows)} lignes existantes).")
else:
    squads_rows = []

# Liste des team_ids restants à traiter
all_team_ids  = df_teams_unique["team_id"].dropna().astype(int).unique().tolist()
todo_team_ids = [tid for tid in all_team_ids if tid not in done_ids]
print(f"\n🔄 {len(todo_team_ids)} team(s) restante(s) sur {len(all_team_ids)} au total.")

for team_id in todo_team_ids:

    if requests_used >= MAX_REQUESTS:
        print(f"\n⛔ Quota atteint. Progression sauvegardée.")
        print(f"   → Relancez le script demain pour continuer ({len(todo_team_ids) - todo_team_ids.index(team_id)} teams restantes).")
        break

    time.sleep(DELAY)
    data = api_get(f"/players/squads?team={team_id}")
    if data is None:
        print(f"⛔ Aucune donnée reçue (quota probablement épuisé). Arrêt immédiat.")
        print(f"   → Relancez demain, {len(todo_team_ids) - todo_team_ids.index(team_id)} teams restantes.")
        break

    for entry in data.get("response", []):
        t = entry.get("team", {})
        for p in entry.get("players", []):
            squads_rows.append({
                "team_id":       t.get("id"),
                "team_name":     t.get("name"),
                "team_logo":     t.get("logo"),
                "player_id":     p.get("id"),
                "player_name":   p.get("name"),
                "player_age":    p.get("age"),
                "player_number": p.get("number"),
                "player_pos":    p.get("position"),
                "player_photo":  p.get("photo"),
            })

    # Sauvegarde progressive après chaque team
    done_ids.add(team_id)
    with open(PROGRESS_FILE, "w") as f:
        json.dump(list(done_ids), f)
    pd.DataFrame(squads_rows).to_csv(SQUADS_CSV, index=False, encoding="utf-8")

df_squads = pd.DataFrame(squads_rows)
print(f"\n✔ df_squads : {len(df_squads)} lignes, {df_squads['player_id'].nunique()} joueurs uniques")

# Nettoyage du fichier de progression si tout est terminé
if set(all_team_ids) == done_ids:
    os.remove(PROGRESS_FILE)
    print("✔ Toutes les teams traitées — fichier de progression supprimé.")