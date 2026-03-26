import http.client
import json
import csv
import os
import time
from dotenv import load_dotenv

# ── 1. Chargement de la clé API depuis .env ──────────────────────────────────
load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")

headers = {
    "x-apisports-key": API_KEY
}

# ── Paramètres de contrôle ───────────────────────────────────────────────────
import pathlib
DATA_DIR = pathlib.Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)
CSV_FILE = DATA_DIR / "API_F_Joueurs.csv"
START_PAGE          = 250     # à modifier pour reprendre après interruption
MAX_REQUESTS        = 2600    # quota dispo pour cette session
DELAY_SECONDS       = 2     # 6s entre requêtes ≈ 10 req/min

FIELDNAMES = [
    "id", "name", "firstname", "lastname",
    "age", "nationality", "height", "weight",
    "position", "photo",
    "birth_date", "birth_place", "birth_country"
]

# ── Fonction : une nouvelle connexion par requête (évite les désordres) ──────
def fetch_page(page: int) -> dict | None:
    try:
        conn = http.client.HTTPSConnection("v3.football.api-sports.io")
        conn.request("GET", f"/players/profiles?page={page}", headers=headers)
        res = conn.getresponse()
        print(f"Page {page} — statut HTTP : {res.status}")
        if res.status != 200:
            print(f"  ⚠ Statut inattendu, page ignorée.")
            return None
        return json.loads(res.read().decode("utf-8"))
    except Exception as e:
        print(f"  ✗ Erreur connexion page {page} : {e}")
        return None
    finally:
        conn.close()

# ── Fonction : écriture des joueurs dans le CSV ──────────────────────────────
def write_players(players: list, writer: csv.DictWriter):
    for entry in players:
        p = entry.get("player", {})
        writer.writerow({
            "id":            p.get("id"),
            "name":          p.get("name"),
            "firstname":     p.get("firstname"),
            "lastname":      p.get("lastname"),
            "age":           p.get("age"),
            "nationality":   p.get("nationality"),
            "height":        p.get("height"),
            "weight":        p.get("weight"),
            "position":      p.get("position"),
            "photo":         p.get("photo"),
            "birth_date":    p.get("birth", {}).get("date"),
            "birth_place":   p.get("birth", {}).get("place"),
            "birth_country": p.get("birth", {}).get("country"),
        })

# ── Récupération de total_pages via la première page de la session ───────────
print(f"Récupération de la page 1 pour initialisation...")
init_data = fetch_page(1)
if init_data is None:
    print("✗ Impossible de récupérer la page initiale. Vérifiez votre clé API.")
    exit(1)

total_pages  = init_data["paging"]["total"]
players_init = init_data.get("response", [])
print(f"  {len(players_init)} joueurs — total pages : {total_pages}")

# ── Ouverture du CSV ─────────────────────────────────────────────────────────
file_exists = os.path.isfile(CSV_FILE)

with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")

    if not file_exists:
        writer.writeheader()

    write_players(players_init, writer)
    requests_used = 1

    # ── Boucle pages suivantes ───────────────────────────────────────────────
    for page in range(START_PAGE + 1, total_pages + 1):

        if requests_used >= MAX_REQUESTS:
            print(f"\n⛔ Limite de {MAX_REQUESTS} requêtes atteinte.")
            print(f"   → Relancez avec START_PAGE = {page}")
            break

        time.sleep(DELAY_SECONDS)

        data = fetch_page(page)
        requests_used += 1

        if data is None:
            continue

        players = data.get("response", [])
        print(f"  {len(players)} joueurs | requêtes restantes : {MAX_REQUESTS - requests_used}")
        write_players(players, writer)

print(f"\n✔ Session terminée — {requests_used} requête(s), données dans '{CSV_FILE}'")