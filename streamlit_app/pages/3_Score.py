"""
pages/3_Score.py — Score Sportif /10 et Score Marketing /10
Football Project — WildCodeSchool 2025-2026

Affiche les scores analytiques précalculés par dbt (déciles NTILE(10)).
Radar sur les composantes brutes de v_joueurs_complets.
Source : v_joueurs_complets
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db import get_joueurs_list, get_joueur_complet

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Score — Football Scout",
    page_icon="🎯",
    layout="wide",
)

st.title("🎯 Scores analytiques")
st.caption("Scores précalculés par décile (1 = bottom 10%, 10 = top 10%) sur l'ensemble de la base.")

# ---------------------------------------------------------------------------
# Sélection du joueur
# ---------------------------------------------------------------------------

df_joueurs = get_joueurs_list()

if df_joueurs.empty:
    st.error("Impossible de charger la liste des joueurs.")
    st.stop()

options = df_joueurs["joueur_id"].tolist()
labels  = (df_joueurs["nom"] + " — " + df_joueurs["club"].fillna("?")).tolist()
id_to_label = dict(zip(options, labels))

# Récupère le joueur sélectionné en Page 2 si disponible
default_id = st.session_state.get("joueur_id_selected", None)
default_index = 0
if default_id and default_id in options:
    default_index = options.index(default_id)

selected_id = st.selectbox(
    "Sélectionner un joueur",
    options=options,
    format_func=lambda x: id_to_label.get(x, str(x)),
    index=default_index,
)

# Choix du profil
profil = st.radio(
    "Afficher le score pour",
    ["⚽ Club (Score Sportif)", "📢 Annonceur (Score Marketing)", "Les deux"],
    horizontal=True,
)

st.divider()

# ---------------------------------------------------------------------------
# Chargement des données
# ---------------------------------------------------------------------------

joueur = get_joueur_complet(selected_id)

if joueur is None:
    st.warning("Aucune donnée trouvée pour ce joueur.")
    st.stop()

def val(col, default=None):
    v = joueur.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return v

nom     = val("nom", "Joueur inconnu")
poste   = val("position", "—")
club    = val("club", "—")
league  = val("league", "—")

score_s       = val("score_sport")
score_s_label = val("score_sport_label", "")
score_m       = val("score_marketing")
score_m_label = val("score_marketing_label", "")

# ---------------------------------------------------------------------------
# En-tête joueur
# ---------------------------------------------------------------------------

st.markdown(f"### {nom}")
st.caption(f"{poste} · {club} · {league}")

st.divider()

# ---------------------------------------------------------------------------
# Helpers affichage
# ---------------------------------------------------------------------------

def affiche_jauge(score, label, couleur, titre):
    """Affiche un score /10 avec jauge visuelle."""
    if score is None:
        st.metric(titre, "N/A")
        st.caption("Données insuffisantes pour calculer ce score.")
        return

    score_int = int(score)

    # Métrique principale — sans delta pour éviter la flèche trompeuse
    st.metric(titre, f"{score_int} / 10")

    # Label du décile en caption
    if label:
        st.caption(f"Décile : {label}")

    # Barre de progression (st.progress attend une valeur 0.0–1.0)
    st.progress(score_int / 10)

    # Interprétation textuelle
    if score_int >= 9:
        interpretation = "🔝 Top 10% — Élite mondiale"
    elif score_int >= 7:
        interpretation = "⭐ Top 30% — Très bon niveau"
    elif score_int >= 5:
        interpretation = "✅ Top 50% — Niveau correct"
    elif score_int >= 3:
        interpretation = "📉 Bottom 40% — En dessous de la moyenne"
    else:
        interpretation = "⚠️ Bottom 20% — Faible niveau relatif"

    st.caption(interpretation)
    st.caption("ℹ️ Score calculé par rapport à l'ensemble des joueurs de la base, même sans données complètes.")


def radar_sportif(joueur, nom):
    """Radar chart des composantes sportives brutes."""
    labels  = ["Note Sofascore", "Buts /90", "Assists /90", "xG", "Saves /90", "Minutes /90"]
    maxima  = [10,               2,          2,             30,   5,            1]
    raw     = [
        float(val("sfs_rating", 0) or 0),
        float(val("goals_p90", 0) or 0),
        float(val("assists_p90", 0) or 0),
        float(val("sfs_expected_goals", 0) or 0),
        float(val("saves_p90", 0) or 0),
        min(float(val("sfs_minutes_played", 0) or 0) / 2700, 1.0),  # normalisé sur saison complète
    ]
    norm = [min(r / m, 1.0) * 10 if m > 0 else 0 for r, m in zip(raw, maxima)]
    norm_closed   = norm + [norm[0]]
    labels_closed = labels + [labels[0]]

    fig = go.Figure(go.Scatterpolar(
        r=norm_closed,
        theta=labels_closed,
        fill="toself",
        name=nom,
        line_color="#1f77b4",
        fillcolor="rgba(31, 119, 180, 0.2)",
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
        height=380,
        title=dict(text="Composantes sportives (normalisées /10)", x=0.5),
    )
    return fig


def radar_marketing(joueur, nom):
    """Radar chart des composantes marketing brutes."""
    instagram = val("instagram")
    twitter   = val("twitter")
    facebook  = val("facebook")
    int_loved = float(val("int_loved", 0) or 0)
    valeur    = float(val("valeur_eur", 0) or 0)

    labels = ["Instagram", "Twitter", "Facebook", "intLoved", "Valeur marchande"]
    raw    = [
        1.0 if instagram else 0.0,
        1.0 if twitter else 0.0,
        1.0 if facebook else 0.0,
        min(int_loved / 15, 1.0) * 10,
        min(valeur / 200_000_000, 1.0) * 10,  # normalisé sur 200M€
    ]
    raw_closed    = raw + [raw[0]]
    labels_closed = labels + [labels[0]]

    fig = go.Figure(go.Scatterpolar(
        r=raw_closed,
        theta=labels_closed,
        fill="toself",
        name=nom,
        line_color="#ff7f0e",
        fillcolor="rgba(255, 127, 14, 0.2)",
    ))
    fig.update_layout(
        polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=20),
        height=380,
        title=dict(text="Composantes marketing (normalisées /10)", x=0.5),
    )
    return fig


# ---------------------------------------------------------------------------
# Affichage selon le profil choisi
# ---------------------------------------------------------------------------

show_sport    = profil in ["⚽ Club (Score Sportif)", "Les deux"]
show_marketing = profil in ["📢 Annonceur (Score Marketing)", "Les deux"]

if show_sport and show_marketing:
    col_s, col_m = st.columns(2)
else:
    col_s = col_m = None

# --- Score Sportif ---
if show_sport:
    container = col_s if col_s else st.container()
    with container:
        st.subheader("⚽ Score Sportif")
        affiche_jauge(score_s, score_s_label, "#1f77b4", "Score Sportif")
        st.divider()
        st.plotly_chart(radar_sportif(joueur, nom), use_container_width=True)

        # Détail des composantes textuelles
        with st.expander("📋 Détail des composantes"):
            composantes = {
                "Note Sofascore":    val("sfs_rating", "—"),
                "Buts /90 min":      val("goals_p90", "—"),
                "Assists /90 min":   val("assists_p90", "—"),
                "xG (saison)":       val("sfs_expected_goals", "—"),
                "Saves /90 min":     val("saves_p90", "—"),
                "Minutes jouées":    val("sfs_minutes_played", "—"),
                "Matchs joués":      val("sfs_appearances", "—"),
            }
            df_comp = pd.DataFrame(
                [(k, v) for k, v in composantes.items()],
                columns=["Composante", "Valeur"]
            )
            st.dataframe(df_comp, hide_index=True, use_container_width=True)

# --- Score Marketing ---
if show_marketing:
    container = col_m if col_m else st.container()
    with container:
        st.subheader("📢 Score Marketing")
        affiche_jauge(score_m, score_m_label, "#ff7f0e", "Score Marketing")
        st.divider()
        st.plotly_chart(radar_marketing(joueur, nom), use_container_width=True)

        with st.expander("📋 Détail des composantes"):
            ligues_premium = {
                "Premier League", "La Liga", "Serie A",
                "Bundesliga", "Ligue 1", "Champions League"
            }
            ligue_joueur = val("league", "")
            ligue_premium_label = (
                f"✅ {ligue_joueur} (ligue premium)"
                if ligue_joueur in ligues_premium
                else f"❌ {ligue_joueur} (hors top 5)"
            )
            composantes_m = {
                "Instagram":          val("instagram", "Non renseigné"),
                "Twitter":            val("twitter", "Non renseigné"),
                "Facebook":           val("facebook", "Non renseigné"),
                "intLoved (0–15)":    val("int_loved", "—"),
                "Ligue (bonus x0.15)": ligue_premium_label,
            }
            df_comp_m = pd.DataFrame(
                [(k, v) for k, v in composantes_m.items()],
                columns=["Composante", "Valeur"]
            )
            st.dataframe(df_comp_m, hide_index=True, use_container_width=True)
