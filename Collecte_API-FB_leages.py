import requests
import pandas as pd
import os
from dotenv import load_dotenv

# ── Configuration ──────────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
}

# ── Appel à l'endpoint /leagues ────────────────────────────────────────────────
url = f"{BASE_URL}/leagues"
params = {
    "type"   : "league",
    "current": "true",
}

response = requests.get(url, headers=HEADERS, params=params)
response.raise_for_status()

data = response.json()

# ── Diagnostic ────────────────────────────────────────────────────────────────
print(f"Status HTTP      : {response.status_code}")
print(f"Requêtes restantes : {response.headers.get('x-ratelimit-requests-remaining')}")
print(f"Résultats reçus  : {data.get('results', 0)}")

# Affiche les erreurs renvoyées par l'API s'il y en a
errors = data.get("errors", {})
if errors:
    print(f"\n⚠️  Erreurs API : {errors}")

# Affiche les 2 premiers éléments bruts pour vérifier la structure
response_list = data.get("response", [])
print(f"\nNombre d'éléments dans 'response' : {len(response_list)}")
if response_list:
    print("\nExemple du 1er élément :")
    import json
    print(json.dumps(response_list[0], indent=2))
else:
    print("\n⚠️  La liste 'response' est vide.")
    print("Conseil : essaie sans les paramètres 'type' et 'current' pour tester :")
    print("  GET /leagues  (sans filtre)")

# ── Aplatissement uniquement si des données existent ──────────────────────────
if not response_list:
    print("\nArrêt : aucune donnée à traiter.")
else:
    rows = []
    for item in response_list:
        league  = item.get("league",  {})
        country = item.get("country", {})
        seasons = item.get("seasons", [])

        current_seasons = [s for s in seasons if s.get("current")]
        current_season  = current_seasons[0] if current_seasons else {}

        row = {
            "league_id"           : league.get("id"),
            "league_name"         : league.get("name"),
            "league_type"         : league.get("type"),
            "league_logo"         : league.get("logo"),
            "country_name"        : country.get("name"),
            "country_code"        : country.get("code"),
            "country_flag"        : country.get("flag"),
            "season_year"         : current_season.get("year"),
            "season_start"        : current_season.get("start"),
            "season_end"          : current_season.get("end"),
            "season_current"      : current_season.get("current"),
            "coverage_standings"  : current_season.get("coverage", {}).get("standings"),
            "coverage_players"    : current_season.get("coverage", {}).get("players"),
            "coverage_top_scorers": current_season.get("coverage", {}).get("top_scorers"),
            "coverage_injuries"   : current_season.get("coverage", {}).get("injuries"),
            "coverage_predictions": current_season.get("coverage", {}).get("predictions"),
            "coverage_odds"       : current_season.get("coverage", {}).get("odds"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    df["season_start"] = pd.to_datetime(df["season_start"], errors="coerce")
    df["season_end"]   = pd.to_datetime(df["season_end"],   errors="coerce")
    df = df.sort_values("league_name").reset_index(drop=True)

    print(f"\nDataFrame : {df.shape[0]} ligues × {df.shape[1]} colonnes")
    print(df.head(10).to_string())

    import pathlib
    DATA_DIR = pathlib.Path(__file__).parent / "data"
    DATA_DIR.mkdir(exist_ok=True)
    df.to_csv(DATA_DIR / "API_F_Championnats.csv", index=False)