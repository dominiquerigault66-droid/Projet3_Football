"""
pages/4_Fiche_joueur.py — Profil complet d'un joueur
Football Project — WildCodeSchool 2025-2026

Accessible depuis la Page 2 (session_state) ou via recherche directe par nom.
Source : v_joueurs_complets (vue dbt dénormalisée)
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db import get_joueurs_list, get_joueur_complet

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Fiche joueur — Football Scout",
    page_icon="👤",
    layout="wide",
)

st.title("👤 Fiche joueur")

# ---------------------------------------------------------------------------
# Sélection du joueur
# ---------------------------------------------------------------------------

df_joueurs = get_joueurs_list()

# Si navigation depuis Page 2 via session_state
default_id = st.session_state.get("joueur_id_selected", None)

if not df_joueurs.empty:
    # Construit le mapping nom → joueur_id pour le selectbox
    options = df_joueurs["joueur_id"].tolist()
    labels  = (df_joueurs["nom"] + " — " + df_joueurs["club"].fillna("?")).tolist()
    id_to_label = dict(zip(options, labels))

    # Index par défaut si navigation depuis Page 2
    default_index = 0
    if default_id and default_id in options:
        default_index = options.index(default_id)

    selected_id = st.selectbox(
        "Rechercher un joueur",
        options=options,
        format_func=lambda x: id_to_label.get(x, str(x)),
        index=default_index,
    )
else:
    st.error("Impossible de charger la liste des joueurs.")
    st.stop()

# ---------------------------------------------------------------------------
# Chargement des données du joueur sélectionné
# ---------------------------------------------------------------------------

joueur = get_joueur_complet(selected_id)

if joueur is None:
    st.warning("Aucune donnée trouvée pour ce joueur.")
    st.stop()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def val(col, default="—"):
    """Retourne la valeur de la colonne ou un défaut si None/NaN."""
    v = joueur.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return v

def fmt_eur(v, default="—"):
    """Formate un montant en euros."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    try:
        return f"{int(v):,} €".replace(",", " ")
    except Exception:
        return str(v)

# ---------------------------------------------------------------------------
# EN-TÊTE : photo + identité
# ---------------------------------------------------------------------------

st.divider()
col_photo, col_info, col_scores = st.columns([1, 3, 2])

with col_photo:
    photo = val("photo_url", None)
    if photo and photo != "—":
        st.image(photo, width=150)
    else:
        st.markdown("🏃 *Photo non disponible*")

with col_info:
    st.markdown(f"## {val('nom')}")
    st.markdown(
        f"**{val('position')}** · {val('position_detail')}  \n"
        f"🏟️ {val('club')} — {val('league')}  \n"
        f"🌍 {val('nationality')} · {val('age', '?')} ans  \n"
        f"👕 N° {val('player_number')}"
    )

with col_scores:
    st.markdown("### Scores analytiques")
    score_s = val("score_sport", None)
    score_m = val("score_marketing", None)

    col_s, col_m = st.columns(2)
    with col_s:
        if score_s is not None and score_s != "—":
            st.metric("⚽ Score Sportif", f"{int(score_s)}/10")
            st.caption(val("score_sport_label", ""))
        else:
            st.metric("⚽ Score Sportif", "N/A")
            st.caption("Données insuffisantes")
    with col_m:
        if score_m is not None and score_m != "—":
            st.metric("📢 Score Marketing", f"{int(score_m)}/10")
            st.caption(val("score_marketing_label", ""))
        else:
            st.metric("📢 Score Marketing", "N/A")
            st.caption("Données insuffisantes")

# ---------------------------------------------------------------------------
# SECTION 1 : Profil physique
# ---------------------------------------------------------------------------

st.divider()
st.subheader("📐 Profil physique")

c1, c2, c3 = st.columns(3)
c1.metric("Taille", f"{val('height_cm')} cm" if val('height_cm') != "—" else "—")
c2.metric("Poids", f"{val('weight_kg')} kg" if val('weight_kg') != "—" else "—")
c3.metric("Pied fort", val("pied_fort"))

# ---------------------------------------------------------------------------
# SECTION 2 : Statistiques saison
# ---------------------------------------------------------------------------

st.divider()
st.subheader("📈 Statistiques saison")

# Colonnes stats disponibles
stats_cols = {
    "Buts":          val("sfs_goals", 0),
    "Passes déc.":   val("sfs_assists", 0),
    "Note":          val("sfs_rating", 0),
    "xG":            val("sfs_expected_goals", 0),
    "Mins jouées":   val("sfs_minutes_played", 0),
    "Buts/90":       val("goals_p90", 0),
    "Assists/90":    val("assists_p90", 0),
}

# Vérifie si des stats existent
has_stats = any(
    v not in (0, "—", None) and not (isinstance(v, float) and pd.isna(v))
    for v in stats_cols.values()
)

if has_stats:
    col_radar, col_metrics = st.columns([2, 1])

    with col_radar:
        # Radar chart — normalise chaque valeur sur son max théorique
        radar_labels = ["Buts", "Passes déc.", "Note /10", "xG", "Buts/90", "Assists/90"]
        maxima       = [50,     30,             10,         30,   2,          2]
        raw_values   = [
            float(val("sfs_goals", 0) or 0),
            float(val("sfs_assists", 0) or 0),
            float(val("sfs_rating", 0) or 0),
            float(val("sfs_expected_goals", 0) or 0),
            float(val("goals_p90", 0) or 0),
            float(val("assists_p90", 0) or 0),
        ]
        norm_values = [min(r / m, 1.0) * 10 for r, m in zip(raw_values, maxima)]
        norm_values_closed = norm_values + [norm_values[0]]
        labels_closed      = radar_labels + [radar_labels[0]]

        fig_radar = go.Figure(go.Scatterpolar(
            r=norm_values_closed,
            theta=labels_closed,
            fill="toself",
            name=val("nom"),
            line_color="#1f77b4",
        ))
        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 10])),
            showlegend=False,
            margin=dict(l=20, r=20, t=30, b=20),
            height=350,
        )
        st.plotly_chart(fig_radar, use_container_width=True)

    with col_metrics:
        st.markdown("**Chiffres clés**")
        st.metric("⚽ Buts", val("sfs_goals", 0))
        st.metric("🎯 Passes déc.", val("sfs_assists", 0))
        st.metric("⭐ Note Sofascore", val("sfs_rating", "—"))
        st.metric("📐 xG", val("sfs_expected_goals", "—"))
        st.metric("🕐 Minutes jouées", val("sfs_minutes_played", "—"))
        st.metric("🟨 Cartons jaunes", val("sfs_yellow_cards", 0))
        st.metric("🟥 Cartons rouges", val("sfs_red_cards", 0))

    # Tableau détaillé dans un expander
    with st.expander("📋 Toutes les statistiques Sofascore"):
        all_stats = {
            "Matchs joués":        val("sfs_appearances"),
            "Minutes jouées":      val("sfs_minutes_played"),
            "Buts":                val("sfs_goals"),
            "Passes décisives":    val("sfs_assists"),
            "Buts + Assists /90":  val("goals_assists_p90"),
            "Buts /90":            val("goals_p90"),
            "Assists /90":         val("assists_p90"),
            "Saves /90":           val("saves_p90"),
            "xG":                  val("sfs_expected_goals"),
            "xA":                  val("sfs_expected_assists"),
            "Note Sofascore":      val("sfs_rating"),
            "Cartons jaunes":      val("sfs_yellow_cards"),
            "Cartons rouges":      val("sfs_red_cards"),
        }
        df_stats = pd.DataFrame(
            [(k, v) for k, v in all_stats.items()],
            columns=["Statistique", "Valeur"]
        )
        st.dataframe(df_stats, hide_index=True, use_container_width=True)

else:
    st.info("Aucune statistique Sofascore disponible pour ce joueur.")

# ---------------------------------------------------------------------------
# SECTION 3 : Finances & contrat
# ---------------------------------------------------------------------------

st.divider()
st.subheader("💶 Finances & contrat")

c1, c2, c3 = st.columns(3)
c1.metric("Valeur marchande", fmt_eur(val("valeur_eur", None)))
c2.metric("Salaire brut / semaine", fmt_eur(val("salaire_brut_semaine_eur", None)))
c3.metric("Salaire brut / an", fmt_eur(val("salaire_brut_annuel_eur", None)))

contrat = val("contrat_expiration_date", None)
if contrat and contrat != "—":
    st.info(f"📅 Contrat expirant le : **{contrat}**")
else:
    st.info("📅 Date de fin de contrat non disponible.")

tm_url = val("tm_player_url", None)
if tm_url and tm_url != "—":
    st.markdown(f"[🔗 Voir sur Transfermarkt]({tm_url})")

# ---------------------------------------------------------------------------
# SECTION 4 : Notoriété & réseaux sociaux
# ---------------------------------------------------------------------------

st.divider()
st.subheader("📢 Notoriété & réseaux sociaux")

col_social, col_bio = st.columns([1, 2])

with col_social:
    insta   = val("instagram", None)
    twitter = val("twitter", None)
    fb      = val("facebook", None)
    loved   = val("int_loved", None)

    if insta and insta != "—":
        st.link_button("📸 Instagram", f"https://www.instagram.com/{insta}")
    if twitter and twitter != "—":
        st.link_button("🐦 Twitter / X", f"https://twitter.com/{twitter}")
    if fb and fb != "—":
        st.link_button("👥 Facebook", f"https://www.facebook.com/{fb}")

    if loved and loved != "—":
        try:
            st.metric("❤️ Score popularité (intLoved)", f"{int(loved)}/15")
        except Exception:
            pass

    if not any([insta, twitter, fb]) or all(v in (None, "—") for v in [insta, twitter, fb]):
        st.caption("Aucun réseau social référencé.")

with col_bio:
    bio_en = val("description_en", None)
    bio_fr = val("description_fr", None)
    bio    = bio_fr if (bio_fr and bio_fr != "—") else bio_en
    if bio and bio != "—":
        st.markdown("**Biographie**")
        st.markdown(bio)
    else:
        st.caption("Aucune biographie disponible.")
