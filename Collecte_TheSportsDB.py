import requests
import pandas as pd
import time
import os
from dotenv import load_dotenv
import pathlib
DATA_DIR = pathlib.Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# Charger la clé API depuis le fichier .env
load_dotenv()
API_KEY = os.environ.get("THESPORTSDB_API_KEY", "3")
BASE_URL = f"https://www.thesportsdb.com/api/v1/json/{API_KEY}"

print(f"API Key: {'***' + API_KEY[-3:] if len(API_KEY) > 3 else 'FREE'}")
print(f"Base URL OK")

# Top 20 FIFA masculin 2026 — mapping pays → championnat principal
mapping_pays_ligues = [
    {"pays": "Spain", "ligue": "Spanish La Liga"},
    {"pays": "Argentina", "ligue": "Argentinian Primera Division"},
    {"pays": "France", "ligue": "French Ligue 1"},
    {"pays": "England", "ligue": "English Premier League"},
    {"pays": "Brazil", "ligue": "Brazilian Serie A"},
    {"pays": "Portugal", "ligue": "Portuguese Primeira Liga"},
    {"pays": "Netherlands", "ligue": "Dutch Eredivisie"},
    {"pays": "Morocco", "ligue": "Moroccan Botola Pro"},
    {"pays": "Belgium", "ligue": "Belgian Pro League"},
    {"pays": "Germany", "ligue": "German Bundesliga"},
    {"pays": "Croatia", "ligue": "Croatian First Football League"},
    {"pays": "Senegal", "ligue": "Senegal Premier League"},
    {"pays": "Italy", "ligue": "Italian Serie A"},
    {"pays": "Colombia", "ligue": "Colombian Primera A"},
    {"pays": "USA", "ligue": "American Major League Soccer"},
    {"pays": "Mexico", "ligue": "Mexican Primera League"},
    {"pays": "Uruguay", "ligue": "Uruguayan Primera Division"},
    {"pays": "Switzerland", "ligue": "Swiss Super League"},
    {"pays": "Japan", "ligue": "Japanese J League"},
    {"pays": "Iran", "ligue": "Iran Pro League"},
]

df_mapping = pd.DataFrame(mapping_pays_ligues)
print(f"{len(df_mapping)} pays/ligues dans le mapping")
df_mapping

# Phase 1 — Validation : tester quelles ligues sont disponibles dans l'API

resultats_ligues = []

for item in mapping_pays_ligues:
    pays = item["pays"]
    ligue = item["ligue"]

    try:
        resp = requests.get(f"{BASE_URL}/search_all_teams.php?l={ligue}", timeout=20)
        if resp.status_code == 429:
            print(f"  Rate limit atteint, pause 15s...")
            time.sleep(15)
            resp = requests.get(f"{BASE_URL}/search_all_teams.php?l={ligue}", timeout=20)

        data = resp.json() if resp.status_code == 200 and resp.text.strip() else {}
        nb_equipes = len(data["teams"]) if data.get("teams") else 0
    except Exception as e:
        print(f"  Erreur pour {ligue}: {e}")
        nb_equipes = 0

    resultats_ligues.append({"pays": pays, "ligue": ligue, "nb_equipes": nb_equipes})
    status = "OK" if nb_equipes > 0 else "NON TROUVEE"
    print(f"{pays:15s} | {ligue:40s} | {nb_equipes:3d} équipes | {status}")
    time.sleep(0.7)

df_ligues = pd.DataFrame(resultats_ligues)
df_ligues_valides = df_ligues[df_ligues["nb_equipes"] > 0].copy()

print(f"\n{len(df_ligues_valides)}/{len(df_ligues)} ligues exploitables")
df_ligues_valides

# Phase 1 — Validation : tester quelles ligues sont disponibles dans l'API

resultats_ligues = []

for item in mapping_pays_ligues:
    pays = item["pays"]
    ligue = item["ligue"]

    try:
        resp = requests.get(f"{BASE_URL}/search_all_teams.php?l={ligue}", timeout=20)
        if resp.status_code == 429:
            print(f"  Rate limit atteint, pause 15s...")
            time.sleep(15)
            resp = requests.get(f"{BASE_URL}/search_all_teams.php?l={ligue}", timeout=20)

        data = resp.json() if resp.status_code == 200 and resp.text.strip() else {}
        nb_equipes = len(data["teams"]) if data.get("teams") else 0
    except Exception as e:
        print(f"  Erreur pour {ligue}: {e}")
        nb_equipes = 0

    resultats_ligues.append({"pays": pays, "ligue": ligue, "nb_equipes": nb_equipes})
    status = "OK" if nb_equipes > 0 else "NON TROUVEE"
    print(f"{pays:15s} | {ligue:40s} | {nb_equipes:3d} équipes | {status}")
    time.sleep(0.7)

df_ligues = pd.DataFrame(resultats_ligues)
df_ligues_valides = df_ligues[df_ligues["nb_equipes"] > 0].copy()

print(f"\n{len(df_ligues_valides)}/{len(df_ligues)} ligues exploitables")
df_ligues_valides

# Export brut — sauvegarde de df_players_raw avant nettoyage
raw_output_path = DATA_DIR / "joueurs_raw.csv"
df_players_raw.to_csv(raw_output_path, index=False, encoding="utf-8-sig")
print(f"Export brut terminé : {raw_output_path}")
print(f"  {df_players_raw.shape[0]} joueurs × {df_players_raw.shape[1]} colonnes")

# Rechargement depuis le CSV brut (utile pour reprendre sans relancer la collecte)
df_players_raw = pd.read_csv(DATA_DIR / "joueurs_raw.csv", encoding="utf-8-sig")
print(f"Chargement terminé : {df_players_raw.shape[0]} joueurs × {df_players_raw.shape[1]} colonnes")
print(f"Ligues présentes : {df_players_raw['league_name'].nunique()}")
df_players_raw.head()

# Phase 3 — Contrôle qualité

print("Joueurs par ligue :")
print(df_players_raw.groupby(["pays_reference", "league_name"]).size().reset_index(name="nb_joueurs").to_string(index=False))

print(f"\nColonnes disponibles ({len(df_players_raw.columns)}) :")
print(list(df_players_raw.columns))

print(f"\nDoublons (même idPlayer) : {df_players_raw['idPlayer'].duplicated().sum()}")
print(f"Valeurs nulles par colonne clé :")
cols_check = ["strPlayer", "strPosition", "dateBorn", "strNationality", "strHeight", "strWeight"]
for c in cols_check:
    if c in df_players_raw.columns:
        pct = df_players_raw[c].isna().mean() * 100
        print(f"  {c:20s} : {pct:.1f}% manquant")

# Phase 4 — Nettoyage : sélection des colonnes utiles + dédoublonnage

colonnes_utiles = [
    # Identifiants
    "idPlayer", "strPlayer", "strPlayerAlternate", "idAPIfootball", "idESPN"
    # Équipe & ligue
    "team_name", "team_id", "league_name", "pays_reference",
    # Profil joueur
    "strPosition", "strNationality", "dateBorn", "strBirthLocation",
    "strHeight", "strWeight", "strGender", "strSide",
    # Stats & infos sportives
    "strSport", "strNumber", "strSigning", "strWage",
    "strKit",
    # Descriptions
    "strDescriptionEN", "strDescriptionFR",
    # Réseaux sociaux
    "strInstagram", "strTwitter", "strFacebook",
    # Image
    "strThumb", "strCutout",
    # popularité
    "intLoved"

]

# Garder seulement les colonnes qui existent dans le dataframe
colonnes_existantes = [c for c in colonnes_utiles if c in df_players_raw.columns]
df_clean = df_players_raw[colonnes_existantes].copy()

# Dédoublonner sur idPlayer (garder la première occurrence)
avant = len(df_clean)
df_clean = df_clean.drop_duplicates(subset="idPlayer", keep="first")
print(f"Dédoublonnage : {avant} → {len(df_clean)} joueurs ({avant - len(df_clean)} doublons supprimés)")

# Nettoyage des hauteurs/poids (conversion en numérique si possible)
if "strHeight" in df_clean.columns:
    df_clean["height_m"] = df_clean["strHeight"].str.extract(r'(\d+\.?\d*)').astype(float)
if "strWeight" in df_clean.columns:
    df_clean["weight_kg"] = df_clean["strWeight"].str.extract(r'(\d+\.?\d*)').astype(float)

# Calcul de l'âge
if "dateBorn" in df_clean.columns:
    df_clean["dateBorn"] = pd.to_datetime(df_clean["dateBorn"], errors="coerce")
    df_clean["age"] = ((pd.Timestamp.now() - df_clean["dateBorn"]).dt.days / 365.25).round(1)

print(f"\nDataFrame nettoyé : {df_clean.shape[0]} joueurs × {df_clean.shape[1]} colonnes")
df_clean.head()

# Phase 5 — Résumé statistique avant export

print("Distribution par position :")
print(df_clean["strPosition"].value_counts().to_string())

print(f"\nDistribution par ligue :")
print(df_clean.groupby("pays_reference").size().sort_values(ascending=False).to_string())

if "age" in df_clean.columns:
    print(f"\nÂge moyen : {df_clean['age'].mean():.1f} ans")
    print(f"Âge min/max : {df_clean['age'].min():.0f} / {df_clean['age'].max():.0f}")

print(f"\nNationalités distinctes : {df_clean['strNationality'].nunique()}")
print(f"Top 10 nationalités :")
print(df_clean["strNationality"].value_counts().head(10).to_string())

# Phase 6 — Export CSV

output_path = DATA_DIR / "TheSportsDB_joueurs_top20_fifa_2026.csv"
df_clean.to_csv(output_path, index=False, encoding="utf-8-sig")
print(f"Export terminé : {output_path}")
print(f"  {df_clean.shape[0]} joueurs × {df_clean.shape[1]} colonnes")
print(f"  Taille : {round(df_clean.memory_usage(deep=True).sum() / 1024 / 1024, 2)} Mo en mémoire")