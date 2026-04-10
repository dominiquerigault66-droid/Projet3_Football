"""
db.py — Connexion MySQL et fonctions utilitaires
Football Project — WildCodeSchool 2025-2026

Utilise SQLAlchemy + PyMySQL. Les credentials sont lus depuis le fichier .env
à la racine du projet.

Dépendances : sqlalchemy, pymysql, python-dotenv, pandas, streamlit
"""

import os
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Chargement des variables d'environnement
# ---------------------------------------------------------------------------

load_dotenv()  # lit le .env à la racine du projet

DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "football_db")


# ---------------------------------------------------------------------------
# Moteur SQLAlchemy (singleton mis en cache par Streamlit)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_engine():
    """
    Crée et retourne le moteur SQLAlchemy (une seule instance par session).
    PyMySQL est utilisé comme driver.
    Le pool_pre_ping=True évite les erreurs de connexion périmée.
    """
    url = (
        f"mysql+pymysql://{DB_USER}:{DB_PASS}"
        f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        f"?charset=utf8mb4"
    )
    engine = create_engine(url, pool_pre_ping=True)
    return engine


# ---------------------------------------------------------------------------
# Fonction générique de requête
# ---------------------------------------------------------------------------

def run_query(sql: str, params: dict = None) -> pd.DataFrame:
    """
    Exécute une requête SQL et retourne un DataFrame pandas.

    Args:
        sql    : requête SQL sous forme de chaîne (paramètres :nom)
        params : dictionnaire de paramètres optionnels

    Returns:
        pd.DataFrame (vide si aucun résultat)

    Exemple :
        df = run_query(
            "SELECT * FROM joueurs WHERE position = :pos LIMIT 10",
            {"pos": "Attaquant"}
        )
    """
    engine = get_engine()
    try:
        with engine.connect() as conn:
            result = conn.execute(text(sql), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
        return df
    except Exception as e:
        st.error(f"Erreur SQL : {e}")
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Requêtes mises en cache (données stables, ne changent pas en cours de session)
# ---------------------------------------------------------------------------

@st.cache_data(ttl=3600)
def get_joueurs_list() -> pd.DataFrame:
    """
    Retourne la liste complète des joueurs pour alimenter les selectbox.
    Colonnes : joueur_id, nom, club, position
    Triée alphabétiquement par nom.
    """
    sql = """
        SELECT joueur_id, nom, club, position
        FROM joueurs
        WHERE nom IS NOT NULL
        ORDER BY nom
    """
    return run_query(sql)


@st.cache_data(ttl=3600)
def get_ligues_list() -> list:
    """Retourne la liste triée des ligues distinctes présentes en base."""
    sql = "SELECT DISTINCT league FROM joueurs WHERE league IS NOT NULL ORDER BY league"
    df = run_query(sql)
    return df["league"].tolist() if not df.empty else []


@st.cache_data(ttl=3600)
def get_nationalites_list() -> list:
    """Retourne la liste triée des nationalités distinctes présentes en base."""
    sql = """
        SELECT DISTINCT nationality
        FROM joueurs
        WHERE nationality IS NOT NULL
        ORDER BY nationality
    """
    df = run_query(sql)
    return df["nationality"].tolist() if not df.empty else []


@st.cache_data(ttl=3600)
def get_joueur_complet(joueur_id: int) -> pd.Series | None:
    """
    Retourne toutes les données d'un joueur depuis v_joueurs_complets.
    Utilisé par la Page 4 (Fiche joueur) et la Page 3 (Score).

    Returns:
        pd.Series avec toutes les colonnes de v_joueurs_complets,
        ou None si joueur_id introuvable.
    """
    sql = """
        SELECT *
        FROM v_joueurs_complets
        WHERE joueur_id = :joueur_id
        LIMIT 1
    """
    df = run_query(sql, {"joueur_id": joueur_id})
    if df.empty:
        return None
    return df.iloc[0]


@st.cache_data(ttl=3600)
def get_stats_accueil() -> dict:
    """
    Retourne les KPIs et données agrégées pour la Page 1 (Accueil).
    Un seul appel pour regrouper les métriques globales.
    """
    stats = {}

    stats["nb_joueurs"] = run_query(
        "SELECT COUNT(*) AS n FROM joueurs"
    ).iloc[0]["n"]

    stats["nb_ligues"] = run_query(
        "SELECT COUNT(DISTINCT league) AS n FROM joueurs WHERE league IS NOT NULL"
    ).iloc[0]["n"]

    stats["repartition_postes"] = run_query(
        "SELECT position, COUNT(*) AS nb FROM joueurs GROUP BY position ORDER BY nb DESC"
    )

    stats["repartition_ligues"] = run_query(
        "SELECT league, COUNT(*) AS nb FROM joueurs WHERE league IS NOT NULL "
        "GROUP BY league ORDER BY nb DESC"
    )

    stats["distribution_ages"] = run_query(
        "SELECT TIMESTAMPDIFF(YEAR, birth_date, CURDATE()) AS age, COUNT(*) AS nb "
        "FROM joueurs WHERE birth_date IS NOT NULL "
        "GROUP BY age ORDER BY age"
    )

    stats["top_nationalites"] = run_query(
        "SELECT nationality, COUNT(*) AS nb FROM joueurs "
        "WHERE nationality IS NOT NULL "
        "GROUP BY nationality ORDER BY nb DESC LIMIT 10"
    )

    return stats


# ---------------------------------------------------------------------------
# Fonction de test de connexion (utile au démarrage)
# ---------------------------------------------------------------------------

def test_connexion() -> bool:
    """
    Teste la connexion MySQL. Retourne True si OK, False sinon.
    Affiche un message Streamlit selon le résultat.
    """
    try:
        df = run_query("SELECT 1 AS ok")
        if not df.empty:
            return True
    except Exception:
        pass
    return False
