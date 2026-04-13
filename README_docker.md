# Docker — Containerisation locale

> **Objectif pédagogique** : cette section démontre la containerisation de la stack complète (Streamlit + MySQL) en local, sans déploiement cloud. Deux conteneurs Docker communiquent via un réseau interne isolé.

---

## Architecture Docker

```
┌─────────────────────────────────────────────────┐
│              docker-compose.yml                  │
│                                                  │
│   ┌──────────────────┐   ┌──────────────────┐   │
│   │  football_mysql  │   │football_streamlit│   │
│   │  (mysql:8.0)     │◄──│  (Dockerfile)    │   │
│   │  port 3307:3306  │   │  port 8501:8501  │   │
│   └────────┬─────────┘   └──────────────────┘   │
│            │                                     │
│       mysql_data (volume persistant)             │
│                                                  │
│           football_network (bridge)              │
└─────────────────────────────────────────────────┘
```

**Points clés de l'architecture :**
- `DB_HOST=mysql` dans le conteneur Streamlit (nom du service Docker, pas `localhost`)
- MySQL exposé sur le port `3307` côté hôte (évite le conflit avec une installation locale sur `3306`)
- `depends_on` + `healthcheck` : Streamlit attend que MySQL soit prêt avant de démarrer
- Volume `mysql_data` : les données survivent aux redémarrages

---

## Fichiers ajoutés

```
Projet3_Football/
├── Dockerfile                  # Image Streamlit (Python 3.13 slim)
├── docker-compose.yml          # Orchestration Streamlit + MySQL
└── requirements_streamlit.txt  # Dépendances allégées (sans scrapers ni Prefect)
```

### `requirements_streamlit.txt` vs `requirements.txt`

`requirements.txt` contient **toutes** les dépendances du projet (scrapers, Selenium, Botasaurus, Prefect…), ce qui représente plusieurs centaines de Mo. Pour l'image Docker de production, on utilise `requirements_streamlit.txt`, qui ne contient que les dépendances nécessaires à Streamlit :

| Package | Utilité |
|---|---|
| `streamlit` | Interface web |
| `plotly` | Graphiques interactifs |
| `SQLAlchemy` + `PyMySQL` | Connexion MySQL |
| `pandas` / `numpy` | Manipulation des données |
| `python-dotenv` | Lecture du `.env` |

---

## Lancement en local

### Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installé et démarré
- Fichier `data/recap_joueurs_clean.csv` présent (partagé via Google Drive)
- Fichier `.env` à la racine du projet

### Étape 1 — Vérifier le fichier `.env`

```
DB_USER=football_user
DB_PASS=football_root
DB_HOST=localhost        # ignoré par Docker (remplacé par le nom du service)
DB_PORT=3307             # port hôte pour se connecter depuis l'extérieur
DB_NAME=football_db
```

### Étape 2 — Construire et démarrer la stack

```bash
# Depuis la racine du projet
docker compose up --build
```

La première fois, Docker :
1. Télécharge `mysql:8.0` (~600 Mo)
2. Construit l'image Streamlit à partir du `Dockerfile` (~300 Mo)
3. Démarre MySQL et exécute `init_db.sql` automatiquement
4. Attend que MySQL soit healthy (healthcheck toutes les 10 s)
5. Démarre Streamlit

Durée estimée du premier lancement : **3 à 5 minutes** selon la connexion.

### Étape 3 — Importer les données

Dans un **second terminal**, pendant que la stack tourne :

```bash
# Importer les données dans le MySQL conteneurisé
# (le port 3307 est celui exposé par docker-compose.yml)
python import_mysql.py
```

> **Note** : `import_mysql.py` doit utiliser `DB_PORT=3307` pour atteindre le MySQL du conteneur depuis l'hôte. Vérifiez votre `.env`.

### Étape 4 — Lancer les modèles dbt

```bash
cd dbt_football
# Adapter profiles.yml : host=localhost, port=3307
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Étape 5 — Accéder à l'application

```
http://localhost:8501
```

---

## Commandes utiles

```bash
# Voir les logs en temps réel
docker compose logs -f

# Voir les logs d'un seul service
docker compose logs -f streamlit
docker compose logs -f mysql

# Vérifier l'état des conteneurs
docker compose ps

# Redémarrer un seul service (ex : après modification du code)
docker compose restart streamlit

# Arrêter la stack (données conservées)
docker compose down

# Arrêter + supprimer les volumes (repart de zéro)
docker compose down -v

# Rebuild forcé après modification du Dockerfile
docker compose up --build --force-recreate
```

---

## Connexion directe à MySQL (debug)

```bash
# Depuis l'hôte (port 3307)
mysql -h 127.0.0.1 -P 3307 -u football_user -pfootball_root football_db

# Depuis l'intérieur du conteneur MySQL
docker exec -it football_mysql mysql -u root -pfootball_root football_db
```

---

## Troubleshooting

| Problème | Solution |
|---|---|
| `Error: mysql: Connection refused` | Attendre que le healthcheck passe (30–60 s au premier démarrage) |
| `Port 3307 already in use` | Changer `3307:3306` en `3308:3306` dans `docker-compose.yml` |
| `ModuleNotFoundError` dans Streamlit | Vérifier `requirements_streamlit.txt` et rebuilder avec `--build` |
| Données perdues après `docker compose down` | Normal si `down -v` — le volume `mysql_data` a été supprimé |
| `DB_HOST=localhost` ne fonctionne pas dans Docker | Dans le conteneur, utiliser `DB_HOST=mysql` (nom du service) |

---

## Pourquoi Docker ici ?

| Avantage | Détail |
|---|---|
| **Reproductibilité** | N'importe quel membre de l'équipe lance la stack identique en une commande |
| **Isolation** | Pas de conflit avec MySQL ou Python installés localement |
| **Portabilité** | Le `Dockerfile` et `docker-compose.yml` suffisent à recréer l'environnement |
| **CV / portfolio** | Démontre la maîtrise de la containerisation sans infrastructure cloud |
