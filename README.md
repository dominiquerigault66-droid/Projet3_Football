# Projet 3 — Analyse de données football

Pipeline complet de collecte, fusion et nettoyage de données sur les joueurs de football professionnel, à partir de 8 sources hétérogènes.

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
projet3/
├── data/                                        # Fichiers CSV (non versionnés)
│   └── transfermarkt/                           # Sous-dossier Transfermarkt + enriched
├── Collecte_API_Football_joueurs.py             # Collecte API-Football joueurs
├── Collecte_API-FB_leages.py                    # Collecte championnats
├── Collecte_API-FB_team.py                      # Collecte équipes et compositions
├── Collecte_ESPN_AF_stats.py                    # Collecte ESPN (effectifs) + API-Football (stats par ligue)
├── Convert_players_to_ESPN_AF_stats.py          # Conversion players.csv → ESPN_AF_stats.csv (si applicable)
├── Fix_ESPN_AF_espn_id.py                       # Renommage colonne espn_id → id dans ESPN_AF_stats.csv
├── Collecte_monScraperFC.py                     # Scraping Transfermarkt
├── Collecte_monScraperFC_enriched.py            # Enrichissement Capology + Sofascore
├── Collecte_TheSportsDB.py                      # Collecte TheSportsDB
├── Merge_joueurs.py                             # Fusion des 8 sources → recap_joueurs.csv
├── Nettoyage_joueurs.py                         # Consolidation → recap_joueurs_clean.csv
├── Inspect_recap_clean.py                       # Rapport dtype / taux de remplissage par colonne
├── requirements.txt
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

Consolide 7 colonnes unifiées (les colonnes sources correspondantes sont supprimées du fichier final) :

| Colonne | Taux de remplissage |
|---|---|
| `birth_date` | ~99,5 % |
| `nationality` | ~98,9 % |
| `position` | ~96,4 % |
| `position_detail` | ~94,8 % |
| `height_cm` | ~89,4 % |
| `club` | ~69,8 % |
| `weight_kg` | ~54,2 % |

---

## Installation

```bash
# Cloner le dépôt
git clone https://github.com/TON_USERNAME/TON_REPO.git
cd TON_REPO

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
THESPORTSDB_API_KEY=ta_clé_thesportsdb
```

---

## Environnement

- Python 3.x
- Windows / Git Bash
- MySQL (phases suivantes)