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
│   ├── transfermarkt/                           # Fichiers intermédiaires : checkpoint.json, scraper.log, players_errors.csv
│   ├── API_F_Joueurs.csv                        # Produit par Collecte_API_Football_joueurs.py
│   ├── API_F_Equipes.csv                        # Produit par Collecte_API-FB_team.py
│   ├── ESPN_AF_stats.csv                        # Produit par Collecte_ESPN_AF_stats.py
│   ├── TheSportsDB_joueurs_top20_fifa_2026.csv  # Produit par Collecte_TheSportsDB.py
│   ├── players_all.csv                          # Produit par Collecte_monScraperFC.py
│   ├── players_enriched.csv                     # Produit par Collecte_monScraperFC_enriched.py
│   ├── recap_joueurs.csv                        # Produit par Merge_joueurs.py
│   └── recap_joueurs_clean.csv                  # Produit par Nettoyage_joueurs.py
├── Collecte_API_Football_joueurs.py             # Collecte API-Football joueurs
├── Collecte_API-FB_leages.py                    # Collecte championnats
├── Collecte_API-FB_team.py                      # Collecte équipes et compositions
├── Collecte_ESPN_AF_stats.py                    # Collecte ESPN (effectifs) + API-Football (stats par ligue)
├── Collecte_monScraperFC.py                     # Scraping Transfermarkt → data/players_all.csv
├── Collecte_monScraperFC_enriched.py            # Enrichissement Capology + Sofascore → data/players_enriched.csv
├── Collecte_TheSportsDB.py                      # Collecte TheSportsDB
├── Merge_joueurs.py                             # Fusion des 8 sources → data/recap_joueurs.csv
├── Nettoyage_joueurs.py                         # Consolidation → data/recap_joueurs_clean.csv
├── pipeline_flow.py                             # Orchestration Prefect — flows hebdo et mensuel
├── init_db.sql                                  # DDL MySQL — création des 7 tables
├── import_mysql.py                              # Import recap_joueurs_clean.csv → MySQL (TRUNCATE + INSERT)
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
Produit `data/recap_joueurs.csv` — **12 959 joueurs × 238 colonnes**, zéro doublon.

Stratégies de jointure :
- Clé normalisée (minuscules, sans accents ni ponctuation) pour la majorité des sources
- Jointure par ID pour Transfermarkt + Enriched
- Double passe pour TheSportsDB (`strPlayer` puis `strPlayerAlternate`)
- Résolution des noms tronqués API-Football Équipes (format `"I. Nom"`) par initiale + nom

### Phase 3 — Nettoyage
```bash
python Nettoyage_joueurs.py
```
Produit `data/recap_joueurs_clean.csv` — **12 959 joueurs × 210 colonnes**.

Consolide 8 colonnes unifiées (les colonnes sources correspondantes sont supprimées du fichier final) :

| Colonne | Taux de remplissage |
|---|---|
| `birth_date` | 99,4 % |
| `nationality` | 98,2 % |
| `position` | 97,9 % |
| `position_detail` | 92,8 % |
| `league` | 92,9 % |
| `height_cm` | 89,0 % |
| `club` | 86,3 % |
| `weight_kg` | 34,3 % |

`league` est consolidée avec la priorité `tm_league → tsdb_league_name → espn_league`, normalisée vers des noms courts standard (`Premier League`, `Ligue 1`, `La Liga`…).

### Phase 4 — Base de données MySQL
```bash
# 1. Créer la base et les tables (une seule fois)
mysql -u root -p < init_db.sql

# 2. Importer les données
python import_mysql.py
```

Alimente les 7 tables du schéma en étoile depuis `recap_joueurs_clean.csv`.
À chaque exécution, toutes les tables sont vidées (`TRUNCATE`) avant réinsertion — pas d'accumulation de doublons.

| Table | Source principale | Couverture |
|---|---|---|
| `joueurs` (centrale) | Toutes sources — nom unifié | 12 959 joueurs |
| `profil` | TheSportsDB, API-Football | ~89 % |
| `clubs` | Transfermarkt, ESPN, apife | ~86 % |
| `valeur_marchande` | Transfermarkt | ~57 % |
| `contrats` | Capology, Transfermarkt | ~55 % |
| `performances` | Sofascore + ESPN | ~4 % (ligues couvertes uniquement) |
| `notoriete` | TheSportsDB | ~98 % |

Voir `README_db.md` pour les instructions détaillées de mise en place.

---

## Orchestration Prefect

Le fichier `pipeline_flow.py` orchestre l'ensemble du pipeline via Prefect 3.

### Flows disponibles

**`flow_hebdo`** — sources à mise à jour hebdomadaire :
API-Football joueurs + équipes, ESPN+AF stats, TheSportsDB → merge → nettoyage → MySQL

**`flow_mensuel`** — toutes les sources :
idem + Transfermarkt + Capology/Sofascore → merge → nettoyage → MySQL

Les collectes s'exécutent en parallèle, sauf `collecte_enriched` qui attend `collecte_transfermarkt` (elle lit `players_all.csv`). Le merge ne démarre qu'une fois toutes les collectes terminées.

### Lancement manuel

```bash
# Flow hebdomadaire
python pipeline_flow.py --flow hebdo

# Flow mensuel complet
python pipeline_flow.py --flow mensuel
```

### Lancement d'une task seule (test ou reprise)

Modifier le bloc `if __name__` en bas de `pipeline_flow.py` :

```python
if __name__ == "__main__":
    collecte_thesportsdb()   # remplacer par la task souhaitée
```

Tasks disponibles : `collecte_apif_joueurs`, `collecte_apif_teams`, `collecte_espn_af`,
`collecte_thesportsdb`, `collecte_transfermarkt`, `collecte_enriched`,
`merge_joueurs`, `nettoyage_joueurs`, `chargement_mysql`.

### Contraintes par source

| Source | Contrainte | Comportement |
|---|---|---|
| API-Football | Quota 100 req/jour (plan gratuit) | Arrêt propre à quota 0, reprise automatique le lendemain |
| ESPN | API publique non documentée | Cache par ligue, pas de requête individuelle |
| TheSportsDB | Rate limiting (429) | Délai 0,7 s entre requêtes, retry automatique |
| Transfermarkt | Délais ScraperFC | 3–8 s/joueur, checkpoint joueur par joueur |
| Capology / Sofascore | Couverture partielle | Retry x2, checkpoint par ligue |
| FBref | CGU restrictives | Exclu du pipeline — données figées |

### Fréquences recommandées

| Flow | Cron | Description |
|---|---|---|
| `flow_hebdo` | `0 3 * * 1` | Chaque lundi à 03h00 |
| `flow_mensuel` | `0 3 1 * *` | 1er de chaque mois à 03h00 |

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

> `requirements.txt` inclut notamment : `pandas`, `requests`, `ScraperFC`, `sqlalchemy`, `pymysql`, `python-dotenv`, `prefect==3.6.24`.

---

## Configuration

Créer un fichier `.env` à la racine du projet :

```
API_FOOTBALL_KEY=ta_clé_api_football
THESPORTSDB_API_KEY=3          # clé publique gratuite (ou ta clé Patreon)
DB_USER=root
DB_PASS=ton_mot_de_passe
DB_HOST=localhost
DB_PORT=3306
DB_NAME=football_db
```

---

## Environnement

- Python 3.13
- Windows / Git Bash
- MySQL 8.0 (phases 4+)
- Orchestration : Prefect 3.6.24 (`@flow` / `@task`, serveur éphémère local)
