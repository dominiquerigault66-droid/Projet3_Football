# Projet 3 — Analyse de données football

Pipeline complet de collecte, fusion, nettoyage et stockage de données sur les joueurs de football professionnel, à partir de 8 sources hétérogènes. Livrable final : une application Streamlit d'aide à la décision pour les clubs (mercato) et les annonceurs (notoriété).

---

## Sources de données

| Source | Fichier produit | Description |
|---|---|---|
| API-Football (joueurs) | `API_F_Joueurs.csv` | Profils joueurs (nom, âge, nationalité, poste, mensurations) |
| API-Football (équipes) | `API_F_Equipes.csv` | Compositions d'équipes par ligue et saison |
| API-Football (championnats) | `API_F_Championnats.csv` | Ligues actives et métadonnées |
| ESPN + API-Football (stats) | `ESPN_AF_stats.csv` | Effectifs ESPN, stats de performance par ligue (goals, assists, rating…) |
| FBref | `FBref_Joueurs.csv` | Stats analytiques avancées (xG, passes progressives) — collecte ponctuelle |
| Transfermarkt (ScraperFC) | `players_all.csv` | Profils, valeurs marchandes, historique transferts |
| Capology + Sofascore (ScraperFC) | `players_enriched.csv` | Salaires et stats de performance avancées |
| TheSportsDB | `TheSportsDB_joueurs_top20_fifa_2026.csv` | Profils joueurs des top 20 nations FIFA, réseaux sociaux |

---

## Structure du projet

```
Projet3_Football/
├── data/                                        # Fichiers CSV (non versionnés — voir .gitignore)
│   └── transfermarkt/                           # Sous-dossier Transfermarkt + enriched
├── Collecte_API_Football_joueurs.py             # Collecte API-Football joueurs
├── Collecte_API-FB_leages.py                    # Collecte championnats
├── Collecte_API-FB_team.py                      # Collecte équipes et compositions
├── Collecte_ESPN_AF_stats.py                    # Collecte ESPN (effectifs) + API-Football (stats par ligue)
├── Collecte_monScraperFC.py                     # Scraping Transfermarkt
├── Collecte_monScraperFC_enriched.py            # Enrichissement Capology + Sofascore
├── Collecte_TheSportsDB.py                      # Collecte TheSportsDB
├── Merge_joueurs.py                             # Fusion des 8 sources → recap_joueurs.csv
├── Nettoyage_joueurs.py                         # Consolidation + league → recap_joueurs_clean.csv
├── init_db.sql                                  # DDL MySQL — création des 7 tables
├── import_mysql.py                              # Import recap_joueurs_clean.csv → MySQL
├── requirements.txt
├── .env                                         # Credentials locaux (non versionné)
└── README.md
```

---

## Pipeline

### Phase 1 — Collecte
Lancer les scripts de collecte indépendamment selon les sources souhaitées. Chaque script produit un CSV dans `data/`.

`Collecte_ESPN_AF_stats.py` fonctionne en trois phases internes :
1. Effectifs ESPN (rosters par équipe, 7 ligues)
2. Stats API-Football chargées en cache par ligue (pas de requête individuelle)
3. Complétion via endpoint ESPN stats pour les joueurs sans correspondance AF

### Phase 2 — Fusion
```bash
python Merge_joueurs.py
```
Produit `data/recap_joueurs.csv` — **16 946 joueurs × 236 colonnes**, zéro doublon.

Stratégies de jointure :
- Clé normalisée (minuscules, sans accents ni ponctuation) pour la majorité des sources
- Jointure par ID pour Transfermarkt + Enriched
- Double passe pour TheSportsDB (`strPlayer` puis `strPlayerAlternate`)
- Résolution des noms tronqués API-Football Équipes (format `"I. Nom"`) par initiale + nom

### Phase 3 — Nettoyage
```bash
python Nettoyage_joueurs.py
```
Produit `data/recap_joueurs_clean.csv` — **16 946 joueurs × 210 colonnes**.

Consolide 8 colonnes unifiées (les colonnes sources correspondantes sont supprimées du fichier final) :

| Colonne | Taux de remplissage |
|---|---|
| `birth_date` | 99,5 % |
| `nationality` | 98,9 % |
| `position` | 96,4 % |
| `position_detail` | 94,8 % |
| `league` | 94,9 % |
| `height_cm` | 89,4 % |
| `club` | 69,8 % |
| `weight_kg` | 54,2 % |

`league` est consolidée avec la priorité `tm_league → tsdb_league_name → espn_league`, normalisée vers des noms courts standard (`Premier League`, `Ligue 1`, `La Liga`…).

### Phase 4 — Base de données MySQL
```bash
# 1. Créer la base et les tables (une seule fois)
mysql -u root -p < init_db.sql

# 2. Importer les données
python import_mysql.py
```

Alimente les 7 tables du schéma en étoile depuis `recap_joueurs_clean.csv` :

| Table | Source principale | Couverture |
|---|---|---|
| `joueurs` (centrale) | Toutes sources — nom unifié | 16 946 joueurs |
| `profil` | TheSportsDB, API-Football | ~89 % |
| `clubs` | Transfermarkt, ESPN, apife | ~70 % |
| `valeur_marchande` | Transfermarkt | ~57 % |
| `contrats` | Capology, Transfermarkt | ~55 % |
| `performances` | Sofascore (32 %) + ESPN (22 %) | ~54 % |
| `notoriete` | TheSportsDB | ~60 % |

Voir `README_db.md` pour les instructions détaillées de mise en place.

---

## Installation

```bash
# Cloner le dépôt
git clone https://github.com/dominiquerigault66-droid/Projet3_Football.git
cd Projet3_Football

# Créer et activer l'environnement virtuel
python -m venv .venv
source .venv/Scripts/activate  # Git Bash / Windows

# Installer les dépendances
pip install -r requirements.txt
```

---

## Configuration

Créer un fichier `.env` à la racine du projet :

```
API_FOOTBALL_KEY=ta_clé_api_football
DB_USER=root
DB_PASS=ton_mot_de_passe
DB_HOST=localhost
DB_PORT=3306
DB_NAME=football_db
```

---

## Environnement

- Python 3.x
- Windows / Git Bash
- MySQL 8.0 (phases 4+)
- Orchestration : Prefect (`@flow` / `@task`)
