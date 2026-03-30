# Base de données MySQL — Football Project

Guide de mise en place locale pour les membres de l'équipe.

## Prérequis

- MySQL 8.0+ installé et démarré
- Python ≥ 3.10 avec le `.venv` du projet activé
- Fichier `data/recap_joueurs_clean.csv` présent (il n'est pas versionné, partagé via Google Drive)

### Installation des dépendances Python

```bash
pip install sqlalchemy pymysql python-dotenv
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

| Étape | Table             | Source principale            | Couverture |
|-------|-------------------|------------------------------|------------|
| 1     | `joueurs`         | Toutes sources (nom unifié)  | 16 946     |
| 2     | `profil`          | TheSportsDB, API-Football    | ~89%       |
| 3     | `clubs`           | Transfermarkt, ESPN, apife   | ~70%       |
| 4     | `valeur_marchande`| Transfermarkt                | ~57%       |
| 5     | `contrats`        | Capology, Transfermarkt      | ~55%       |
| 6     | `performances`    | Sofascore (32%) + ESPN (22%) | ~54%       |
| 7     | `notoriete`       | TheSportsDB                  | ~60%       |

Durée estimée : **3 à 8 minutes** selon les performances de ta machine.

---

## 4. Vérifier l'import

```sql
USE football_db;
SELECT COUNT(*) FROM joueurs;          -- doit être ~16 946
SELECT COUNT(*) FROM performances;    -- doit être ~7 000-9 000
SELECT COUNT(*) FROM notoriete;       -- doit être ~16 946

-- Test rapide : top 10 joueurs par valeur marchande
SELECT j.nom, j.club, j.league, vm.valeur_texte, vm.valeur_eur
FROM joueurs j
JOIN valeur_marchande vm ON j.joueur_id = vm.joueur_id
WHERE vm.valeur_eur IS NOT NULL
ORDER BY vm.valeur_eur DESC
LIMIT 10;
```

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
│   ├── recap_joueurs.csv          # produit par Merge_joueurs.py
│   └── recap_joueurs_clean.csv    # produit par Nettoyage_joueurs.py
├── init_db.sql                    # DDL — crée les tables
├── import_mysql.py                # Import CSV → MySQL
├── Nettoyage_joueurs.py           # Nettoyage + consolidation league
├── .env                           # ⚠️ NON commité (credentials locaux)
└── .gitignore                     # doit contenir : .env, data/*.csv
```

---

## Notes importantes

**`tm_Value`** est parsée automatiquement par `import_mysql.py` :
- `'€12.00m'` → `12 000 000` (stocké dans `valeur_marchande.valeur_eur`)
- `'€700k'` → `700 000`

**`league`** : la colonne est consolidée dans `Nettoyage_joueurs.py` avec la
priorité `tm_league → tsdb_league_name → espn_league`, normalisée vers des
noms courts standard (`Premier League`, `Ligue 1`, etc.).

**`INSERT IGNORE`** : le script est idempotent — on peut le relancer sans
créer de doublons (les lignes existantes sont ignorées).

**Réinitialisation complète** (si besoin de repartir de zéro) :
```bash
mysql -u root -p -e "DROP DATABASE football_db;"
mysql -u root -p < init_db.sql
python import_mysql.py
```
