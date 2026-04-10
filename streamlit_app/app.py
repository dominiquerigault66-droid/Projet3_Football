"""
app.py — Point d'entrée de l'application Streamlit
Football Project — WildCodeSchool 2025-2026

Lancement :
    streamlit run app.py

Structure des pages :
    pages/1_Accueil.py
    pages/2_Recherche.py
    pages/3_Score.py
    pages/4_Fiche_joueur.py
    pages/5_Comparaison.py  (optionnelle)
"""

import streamlit as st
from db import test_connexion

# ---------------------------------------------------------------------------
# Configuration globale de la page
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Football Scout & Media",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# CSS minimal pour harmoniser l'apparence
# ---------------------------------------------------------------------------

st.markdown("""
<style>
    /* Réduit le padding vertical du header */
    .block-container { padding-top: 1.5rem; }
    /* Style des métriques KPI */
    [data-testid="stMetricValue"] { font-size: 2rem; }
</style>
""", unsafe_allow_html=True)

# ---------------------------------------------------------------------------
# En-tête principal
# ---------------------------------------------------------------------------

st.title("⚽ Football Scout & Media")
st.caption("Application d'aide à la décision — Recrutement & Partenariats")

st.divider()

# ---------------------------------------------------------------------------
# Vérification de la connexion MySQL au démarrage
# ---------------------------------------------------------------------------

with st.spinner("Connexion à la base de données..."):
    ok = test_connexion()

if ok:
    st.success("✅ Connecté à `football_db`")
else:
    st.error(
        "❌ Impossible de se connecter à MySQL. "
        "Vérifiez votre fichier `.env` et que le serveur MySQL est démarré."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Présentation des pages sur la page d'accueil
# ---------------------------------------------------------------------------

st.subheader("Navigation")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.info("**📊 Page 1 — Accueil**\n\nKPIs globaux et distributions de la base")

with col2:
    st.info("**🔍 Page 2 — Recherche**\n\nFiltres multi-critères pour clubs et annonceurs")

with col3:
    st.info("**🎯 Page 3 — Score**\n\nScore Sportif /10 et Score Marketing /10")

with col4:
    st.info("**👤 Page 4 — Fiche joueur**\n\nProfil complet : stats, finances, notoriété")

st.markdown("---")
st.caption(
    "Données : API-Football · TheSportsDB · Transfermarkt · Sofascore · Capology  |  "
    "WildCodeSchool 2025-2026  |  Équipe : Carla Godoy · Patricia Ferreira · Dominique Rigault"
)

# ---------------------------------------------------------------------------
# Initialisation du session_state (partagé entre pages)
# ---------------------------------------------------------------------------

# joueur_id sélectionné depuis la Page 2 pour navigation vers Page 4
if "joueur_id_selected" not in st.session_state:
    st.session_state["joueur_id_selected"] = None
