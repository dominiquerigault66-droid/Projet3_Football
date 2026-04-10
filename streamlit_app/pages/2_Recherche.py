"""
pages/2_Recherche.py — Recherche & filtres joueurs
Football Project — WildCodeSchool 2025-2026

Filtres multi-critères en sidebar, tableau de résultats avec bouton
de navigation vers la Page 4 (Fiche joueur) via session_state.
Source : v_joueurs_complets (vue dbt dénormalisée)
"""

import streamlit as st
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db import run_query, get_ligues_list, get_nationalites_list

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Recherche — Football Scout",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 Recherche de joueurs")
st.caption("Utilisez les filtres dans la sidebar pour affiner votre recherche.")

# ---------------------------------------------------------------------------
# SIDEBAR — Filtres
# ---------------------------------------------------------------------------

st.sidebar.header("Filtres")

# Profil utilisateur
profil = st.sidebar.radio(
    "Profil",
    ["Club (sportif)", "Annonceur (notoriété)", "Les deux"],
    horizontal=False,
)

st.sidebar.divider()

# --- Filtres communs ---
st.sidebar.subheader("🔎 Général")

nom_recherche = st.sidebar.text_input("Nom du joueur (contient)", "")

position = st.sidebar.selectbox(
    "Poste",
    ["Tous", "Attacker", "Midfielder", "Defender", "Goalkeeper"],
)

ligues_dispo = get_ligues_list()
ligues_sel = st.sidebar.multiselect("Ligue(s)", ligues_dispo)

nationalites_dispo = get_nationalites_list()
nat_sel = st.sidebar.multiselect("Nationalité(s)", nationalites_dispo)

age_min, age_max = st.sidebar.slider("Tranche d'âge", 15, 45, (15, 45))

# --- Filtres sportifs (Club) ---
if profil in ["Club (sportif)", "Les deux"]:
    st.sidebar.divider()
    st.sidebar.subheader("⚽ Performance sportive")

    buts_min = st.sidebar.number_input("Buts minimum", min_value=0, value=0, step=1)
    passes_min = st.sidebar.number_input("Passes déc. minimum", min_value=0, value=0, step=1)
    note_min = st.sidebar.slider("Note Sofascore minimum", 0.0, 10.0, 0.0, step=0.1)
    xg_min = st.sidebar.number_input("xG minimum", min_value=0.0, value=0.0, step=0.5)
    minutes_min = st.sidebar.number_input("Minutes jouées minimum", min_value=0, value=0, step=90)
    pied_fort = st.sidebar.selectbox("Pied fort", ["Tous", "Right", "Left", "Both"])
else:
    buts_min = 0
    passes_min = 0
    note_min = 0.0
    xg_min = 0.0
    minutes_min = 0
    pied_fort = "Tous"

# --- Filtres notoriété (Annonceur) ---
if profil in ["Annonceur (notoriété)", "Les deux"]:
    st.sidebar.divider()
    st.sidebar.subheader("📢 Notoriété")

    filtre_instagram = st.sidebar.checkbox("Avec compte Instagram")
    filtre_twitter   = st.sidebar.checkbox("Avec compte Twitter")
    intloved_min     = st.sidebar.slider("Score popularité minimum (intLoved)", 0, 15, 0)
else:
    filtre_instagram = False
    filtre_twitter   = False
    intloved_min     = 0

# --- Filtres financiers ---
st.sidebar.divider()
st.sidebar.subheader("💶 Finances")

valeur_max = st.sidebar.number_input(
    "Valeur marchande max (€)", min_value=0, value=0, step=1_000_000,
    help="0 = pas de limite"
)
salaire_max = st.sidebar.number_input(
    "Salaire brut annuel max (€)", min_value=0, value=0, step=10_000,
    help="0 = pas de limite"
)

# ---------------------------------------------------------------------------
# Détection des filtres actifs
# ---------------------------------------------------------------------------

filtres_actifs = any([
    nom_recherche.strip(),
    position != "Tous",
    len(ligues_sel) > 0,
    len(nat_sel) > 0,
    age_min > 15 or age_max < 45,
    buts_min > 0,
    passes_min > 0,
    note_min > 0.0,
    xg_min > 0.0,
    minutes_min > 0,
    pied_fort != "Tous",
    filtre_instagram,
    filtre_twitter,
    intloved_min > 0,
    valeur_max > 0,
    salaire_max > 0,
])

# ---------------------------------------------------------------------------
# Message si aucun filtre actif
# ---------------------------------------------------------------------------

if not filtres_actifs:
    st.info(
        "👈 Activez au moins un filtre dans la sidebar pour lancer la recherche."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Construction de la requête SQL dynamique
# ---------------------------------------------------------------------------

conditions = []
params = {}

# Nom
if nom_recherche.strip():
    conditions.append("nom LIKE :nom")
    params["nom"] = f"%{nom_recherche.strip()}%"

# Poste
if position != "Tous":
    conditions.append("position = :position")
    params["position"] = position

# Ligue
if ligues_sel:
    conditions.append("league IN :ligues")
    params["ligues"] = tuple(ligues_sel)

# Nationalité
if nat_sel:
    conditions.append("nationality IN :nationalites")
    params["nationalites"] = tuple(nat_sel)

# Âge
if age_min > 15 or age_max < 45:
    conditions.append("age BETWEEN :age_min AND :age_max")
    params["age_min"] = age_min
    params["age_max"] = age_max

# Stats sportives
if buts_min > 0:
    conditions.append("(sfs_goals >= :buts_min)")
    params["buts_min"] = buts_min

if passes_min > 0:
    conditions.append("(sfs_assists >= :passes_min)")
    params["passes_min"] = passes_min

if note_min > 0.0:
    conditions.append("(sfs_rating >= :note_min)")
    params["note_min"] = note_min

if xg_min > 0.0:
    conditions.append("(sfs_expected_goals >= :xg_min)")
    params["xg_min"] = xg_min

if minutes_min > 0:
    conditions.append("(sfs_minutes_played >= :minutes_min)")
    params["minutes_min"] = minutes_min

if pied_fort != "Tous":
    conditions.append("pied_fort = :pied_fort")
    params["pied_fort"] = pied_fort

# Notoriété
if filtre_instagram:
    conditions.append("instagram IS NOT NULL AND instagram != ''")

if filtre_twitter:
    conditions.append("twitter IS NOT NULL AND twitter != ''")

if intloved_min > 0:
    conditions.append("(int_loved >= :intloved_min)")
    params["intloved_min"] = intloved_min

# Finances
if valeur_max > 0:
    conditions.append("(valeur_eur <= :valeur_max OR valeur_eur IS NULL)")
    params["valeur_max"] = valeur_max

if salaire_max > 0:
    conditions.append("(salaire_brut_annuel_eur <= :salaire_max OR salaire_brut_annuel_eur IS NULL)")
    params["salaire_max"] = salaire_max

where_clause = " AND ".join(conditions)

sql = f"""
    SELECT
        joueur_id,
        nom,
        club,
        league,
        position,
        age,
        nationality,
        sfs_goals        AS buts,
        sfs_assists      AS passes,
        sfs_rating       AS note,
        sfs_expected_goals AS xG,
        valeur_eur,
        score_sport,
        score_marketing,
        photo_url
    FROM v_joueurs_complets
    WHERE {where_clause}
    ORDER BY valeur_eur DESC
    LIMIT 200
"""

# ---------------------------------------------------------------------------
# Exécution et affichage
# ---------------------------------------------------------------------------

with st.spinner("Recherche en cours..."):
    # SQLAlchemy n'accepte pas tuple directement dans IN — on contourne
    from sqlalchemy import text as sa_text
    from db import get_engine

    engine = get_engine()
    try:
        # Remplace les tuples IN par une interpolation sécurisée
        sql_final = sql
        params_final = {}
        for k, v in params.items():
            if isinstance(v, tuple):
                placeholders = ", ".join([f":_{k}_{i}" for i in range(len(v))])
                sql_final = sql_final.replace(f":{{k}}", placeholders).replace(
                    f"IN :{k}", f"IN ({placeholders})"
                )
                for i, val_item in enumerate(v):
                    params_final[f"_{k}_{i}"] = val_item
            else:
                params_final[k] = v

        with engine.connect() as conn:
            result = conn.execute(sa_text(sql_final), params_final)
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
    except Exception as e:
        st.error(f"Erreur SQL : {e}")
        df = pd.DataFrame()

# ---------------------------------------------------------------------------
# Résultats
# ---------------------------------------------------------------------------

if df.empty:
    st.warning("Aucun joueur ne correspond à ces critères.")
    st.stop()

st.success(f"**{len(df)} joueur(s) trouvé(s)** (limité à 200 résultats, triés par valeur marchande décroissante)")

# Export CSV
csv_data = df.drop(columns=["photo_url", "joueur_id"], errors="ignore").to_csv(index=False).encode("utf-8")
st.download_button(
    label="⬇️ Exporter en CSV",
    data=csv_data,
    file_name="selection_joueurs.csv",
    mime="text/csv",
)

st.divider()

# ---------------------------------------------------------------------------
# Tableau avec bouton Voir fiche par ligne
# ---------------------------------------------------------------------------

# Colonnes à afficher (sans joueur_id et photo_url)
cols_affichage = ["nom", "club", "league", "position", "age", "nationality",
                  "buts", "passes", "note", "xG", "valeur_eur",
                  "score_sport", "score_marketing"]

# Ligne d'en-têtes
h_btn, h_nom, h_club, h_ligue, h_pos, h_age, h_buts, h_passes, h_note, h_val, h_ss, h_sm = st.columns(
    [1, 2, 2, 2, 1.2, 0.8, 0.8, 0.8, 0.8, 1.5, 0.8, 0.8]
)
h_btn.markdown("&nbsp;")
h_nom.markdown("**Joueur**")
h_club.markdown("**Club**")
h_ligue.markdown("**Ligue**")
h_pos.markdown("**Poste**")
h_age.markdown("**Âge**")
h_buts.markdown("**Buts**")
h_passes.markdown("**Passes**")
h_note.markdown("**Note**")
h_val.markdown("**Valeur €**")
h_ss.markdown("**Sport**")
h_sm.markdown("**Mktg**")
st.divider()

for _, row in df.iterrows():
    col_btn, col_nom, col_club, col_ligue, col_pos, col_age, col_buts, col_passes, col_note, col_val, col_ss, col_sm = st.columns(
        [1, 2, 2, 2, 1.2, 0.8, 0.8, 0.8, 0.8, 1.5, 0.8, 0.8]
    )

    with col_btn:
        if st.button("👤 Fiche", key=f"fiche_{row['joueur_id']}"):
            st.session_state["joueur_id_selected"] = int(row["joueur_id"])
            st.switch_page("pages/4_Fiche_joueur.py")

    col_nom.markdown(f"**{row['nom']}**")
    col_club.write(row.get("club", "—") or "—")
    col_ligue.write(row.get("league", "—") or "—")
    col_pos.write(row.get("position", "—") or "—")
    col_age.write(row.get("age", "—") or "—")
    col_buts.write(row.get("buts", "—") if pd.notna(row.get("buts")) else "—")
    col_passes.write(row.get("passes", "—") if pd.notna(row.get("passes")) else "—")
    col_note.write(row.get("note", "—") if pd.notna(row.get("note")) else "—")

    val_eur = row.get("valeur_eur")
    if val_eur and pd.notna(val_eur):
        col_val.write(f"{int(val_eur):,} €".replace(",", " "))
    else:
        col_val.write("—")

    ss = row.get("score_sport")
    col_ss.write(f"{int(ss)}/10" if ss and pd.notna(ss) else "—")

    sm = row.get("score_marketing")
    col_sm.write(f"{int(sm)}/10" if sm and pd.notna(sm) else "—")

    st.divider()
