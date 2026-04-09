# Base de données MySQL — Football Project

Guide de mise en place locale pour les membres de l'équipe.

## Prérequis

- MySQL 8.0+ installé et démarré
- Python ≥ 3.10 avec le `.venv` du projet activé
- Fichier `data/recap_joueurs_clean.csv` présent (il n'est pas versionné, partagé via Google Drive)

### Installation des dépendances Python

```bash
pip install sqlalchemy pymysql python-dotenv dbt-mysql
```

---

## 1. Fichier `.env`

Créer un fichier `.env` à la **racine du projet** (jamais commité sur GitHub — déjà dans `.gitignore`) :

```
DB_USER=root
DB_PASS=ton_mot_de_passe
DB_HOST=localhost
DB_PORT=3306
DB_NAME=football_db
```

> Si tu utilises MAMP / WAMP / XAMPP, le port peut être `3307` — vérifier dans ton interface.

---

## 2. Créer la base et les tables

```bash
mysql -u root -p < init_db.sql
```

Cette commande :
- Crée la base `football_db` si elle n'existe pas
- Crée les 7 tables du schéma en étoile

---

## 3. Importer les données

```bash
python import_mysql.py
```

Le script lit `data/recap_joueurs_clean.csv` et insère les données dans cet ordre :

| Étape | Table              | Source principale              | Couverture                         |
|-------|--------------------|--------------------------------|------------------------------------|
| 1     | `joueurs`          | Toutes sources (nom unifié)    | 12 290                             |
| 2     | `profil`           | TheSportsDB, API-Football      | ~99 %                              |
| 3     | `clubs`            | Transfermarkt, ESPN, apife     | ~99 %                              |
| 4     | `valeur_marchande` | Transfermarkt                  | ~99 %                              |
| 5     | `contrats`         | Capology, Transfermarkt        | ~99 %                              |
| 6     | `performances`     | Sofascore (45 %)               | ~45 % (ligues couvertes seulement) |
| 7     | `notoriete`        | TheSportsDB                    | ~99 %                              |

> **Note performances** : l'endpoint ESPN stats est actuellement indisponible (HTTP 404).
> Les stats de performance proviennent exclusivement de Sofascore via `Collecte_monScraperFC_enriched.py`.

Durée estimée : **3 à 8 minutes** selon les performances de ta machine.

---

## 4. Vérifier l'import

```sql
USE football_db;
SELECT COUNT(*) FROM joueurs;          -- doit être ~12 290
SELECT COUNT(*) FROM performances;    -- doit être ~5 500
SELECT COUNT(*) FROM notoriete;       -- doit être ~12 167

-- Test rapide : top 10 joueurs par valeur marchande
SELECT j.nom, j.club, j.league, vm.valeur_texte, vm.valeur_eur
FROM joueurs j
JOIN valeur_marchande vm ON j.joueur_id = vm.joueur_id
WHERE vm.valeur_eur IS NOT NULL
ORDER BY vm.valeur_eur DESC
LIMIT 10;
```

---

## 5. Modèles dbt Core

dbt Core matérialise 8 modèles analytiques dans `football_db` à partir des 7 tables brutes.

### Lancement

```bash
cd dbt_football
export $(grep -v '^#' ../.env | xargs)   # charge les variables DB dans le shell
dbt run --profiles-dir .                  # crée/met à jour les 8 modèles
dbt test --profiles-dir .                 # valide l'intégrité (10 tests)
```

### Modèles staging (vues)

| Modèle | Description |
|---|---|
| `stg_joueurs` | Table joueurs typée, calcul de l'âge à la date du jour |
| `stg_performances` | Métriques Sofascore + calculs /90 min (buts, assists, saves, passes clés) |
| `stg_scores` | Composantes brutes des scores sportif et marketing avant percentiles |

### Modèles marts (tables physiques)

| Modèle | Lignes | Description |
|---|---|---|
| `v_joueurs_complets` | 12 290 | Vue centrale dénormalisée — toutes les tables jointes + scores S1-S10 / M1-M10 |
| `v_recrutement` | 12 104 | Profil recruteur — performance, contrat, valeur marchande |
| `v_marketing` | 12 290 | Profil annonceur — notoriété, réseaux sociaux, image |
| `v_contrats_expiration` | 5 110 | Contrats expirant dans les 18 mois — opportunités mercato |
| `v_top_valeur_par_poste` | 8 521 | Benchmark valeur marchande par poste et par ligue avec rangs |

### Scores calculés

**Score sportif S1–S10** (percentile par poste, `NTILE(10)`) :

| Poste | Composantes |
|---|---|
| Goalkeeper | rating 35% · saves+clean_sheets/90 30% · régularité 20% · âge inversé 15% |
| Defender | rating 55% · buts+assists/90 10% · régularité 20% · âge inversé 15% |
| Midfielder | rating 45% · buts+assists/90 20% · régularité 20% · âge inversé 15% |
| Attacker | rating 35% · buts+assists/90 30% · régularité 20% · âge inversé 15% |

`NULL` pour les joueurs sans données Sofascore (~55% du total).

**Score marketing M1–M10** (percentile global, `NTILE(10)`) :
- `log(int_loved + 1)` : 55%
- Réseaux sociaux renseignés (0–3 plateformes) : 30%
- Ligue premium (top 5 européennes + Champions League) : 15%

Distribution uniforme garantie : ~1 229 joueurs par décile.

### Intégration Prefect

`dbt_run` est la dernière task du pipeline — elle s'exécute automatiquement après `chargement_mysql` dans `flow_hebdo` et `flow_mensuel`.

---

## Schéma en étoile

```
                    profil
                      │
          clubs ─── joueurs ─── notoriete
                      │
         valeur_marchande
                      │
              contrats
                      │
             performances
```

Table centrale : `joueurs` (clé `joueur_id` AUTO_INCREMENT)
Tables satellites reliées par `joueur_id` (FOREIGN KEY)

---

## Structure des fichiers

```
Projet3_Football/
├── data/
│   ├── transfermarkt/                 # Fichiers intermédiaires (checkpoint, log, errors)
│   ├── recap_joueurs.csv              # Produit par Merge_joueurs.py
│   └── recap_joueurs_clean.csv        # Produit par Nettoyage_joueurs.py
├── dbt_football/                      # Projet dbt Core
│   ├── models/staging/                # stg_joueurs, stg_performances, stg_scores
│   ├── models/marts/                  # v_joueurs_complets, v_recrutement, v_marketing,
│   │                                  # v_contrats_expiration, v_top_valeur_par_poste
│   ├── dbt_project.yml
│   └── profiles.yml
├── init_db.sql                        # DDL — crée les 7 tables brutes
├── import_mysql.py                    # Import CSV → MySQL
├── Nettoyage_joueurs.py               # Nettoyage + consolidation league
├── .env                               # ⚠️ NON commité (credentials locaux)
└── .gitignore                         # doit contenir : .env, data/*.csv
```

---

## Notes importantes

**`tm_Value`** est parsée automatiquement par `import_mysql.py` :
- `'€12.00m'` → `12 000 000` (stocké dans `valeur_marchande.valeur_eur`)
- `'€700k'` → `700 000`

**`league`** : la colonne est consolidée dans `Nettoyage_joueurs.py` avec la
priorité `tm_league → tsdb_league_name → espn_league`, normalisée vers des
noms courts standard (`Premier League`, `Ligue 1`, etc.).

**`TRUNCATE` avant insertion** : le script vide toutes les tables avant chaque
import puis réinsère l'intégralité des données — pas d'accumulation de doublons
entre deux exécutions.

**Réinitialisation complète** (si besoin de repartir de zéro) :
```bash
mysql -u root -p -e "DROP DATABASE football_db;"
mysql -u root -p < init_db.sql
python import_mysql.py
cd dbt_football && export $(grep -v '^#' ../.env | xargs) && dbt run --profiles-dir .
```
