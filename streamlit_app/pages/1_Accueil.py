"""
pages/1_Accueil.py — Tableau de bord global
Football Project — WildCodeSchool 2025-2026

KPIs globaux + 4 graphiques de distribution.
Données issues directement des tables brutes MySQL.
"""

import streamlit as st
import plotly.express as px
import sys
import os

# Permet d'importer db.py depuis le dossier parent (streamlit_app/)
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db import get_stats_accueil

# ---------------------------------------------------------------------------
# Configuration de la page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Accueil — Football Scout",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Vue d'ensemble de la base")
st.caption("Statistiques globales sur l'ensemble des joueurs référencés.")

# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------

with st.spinner("Chargement des données..."):
    stats = get_stats_accueil()

# ---------------------------------------------------------------------------
# KPI Cards
# ---------------------------------------------------------------------------

st.subheader("Chiffres clés")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("👤 Joueurs actifs", f"{stats['nb_joueurs']:,}".replace(",", " "))

with col2:
    st.metric("🏆 Ligues couvertes", stats["nb_ligues"])

with col3:
    st.metric("🗄️ Sources de données", "6")

with col4:
    st.metric("📅 Dernière mise à jour", "2026")

st.divider()

# ---------------------------------------------------------------------------
# Graphiques — ligne 1 : postes et ligues
# ---------------------------------------------------------------------------

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Répartition par poste")
    df_postes = stats["repartition_postes"]
    if not df_postes.empty:
        fig_postes = px.bar(
            df_postes,
            x="nb",
            y="position",
            orientation="h",
            labels={"nb": "Nombre de joueurs", "position": "Poste"},
            color="nb",
            color_continuous_scale="Blues",
        )
        fig_postes.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
        )
        st.plotly_chart(fig_postes, use_container_width=True)
    else:
        st.info("Aucune donnée disponible.")

with col_right:
    st.subheader("Répartition par ligue")
    df_ligues = stats["repartition_ligues"]
    if not df_ligues.empty:
        # Regroupe les ligues avec peu de joueurs dans "Autres"
        seuil = df_ligues["nb"].sum() * 0.02  # < 2% → Autres
        df_ligues_plot = df_ligues.copy()
        df_ligues_plot.loc[df_ligues_plot["nb"] < seuil, "league"] = "Autres"
        df_ligues_plot = (
            df_ligues_plot.groupby("league", as_index=False)["nb"].sum()
        )
        fig_ligues = px.pie(
            df_ligues_plot,
            values="nb",
            names="league",
            hole=0.35,
        )
        fig_ligues.update_layout(
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            legend=dict(orientation="v", x=1, y=0.5),
        )
        st.plotly_chart(fig_ligues, use_container_width=True)
    else:
        st.info("Aucune donnée disponible.")

# ---------------------------------------------------------------------------
# Graphiques — ligne 2 : âges et nationalités
# ---------------------------------------------------------------------------

col_left2, col_right2 = st.columns(2)

with col_left2:
    st.subheader("Distribution des âges")
    df_ages = stats["distribution_ages"]
    if not df_ages.empty:
        fig_ages = px.bar(
            df_ages,
            x="age",
            y="nb",
            labels={"age": "Âge", "nb": "Nombre de joueurs"},
            color="nb",
            color_continuous_scale="Teal",
        )
        fig_ages.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
        )
        st.plotly_chart(fig_ages, use_container_width=True)
    else:
        st.info("Aucune donnée disponible.")

with col_right2:
    st.subheader("Top 10 nationalités")
    df_nat = stats["top_nationalites"]
    if not df_nat.empty:
        fig_nat = px.bar(
            df_nat,
            x="nb",
            y="nationality",
            orientation="h",
            labels={"nb": "Nombre de joueurs", "nationality": "Nationalité"},
            color="nb",
            color_continuous_scale="Oranges",
        )
        fig_nat.update_layout(
            showlegend=False,
            coloraxis_showscale=False,
            margin=dict(l=10, r=10, t=10, b=10),
            height=300,
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig_nat, use_container_width=True)
    else:
        st.info("Aucune donnée disponible.")
