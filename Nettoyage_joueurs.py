import pandas as pd
import numpy as np
import ast, re
import pathlib
DATA_DIR = pathlib.Path(__file__).parent / "data"

recap = pd.read_csv(DATA_DIR / "recap_joueurs.csv", low_memory=False)
print(f"Chargé : {recap.shape[0]} lignes × {recap.shape[1]} colonnes")

# ══════════════════════════════════════════════════════════════════════════════
# 1. DATE DE NAISSANCE
# ══════════════════════════════════════════════════════════════════════════════

def parse_date(s):
    """Retourne une date au format YYYY-MM-DD ou NaT."""
    if pd.isna(s):
        return pd.NaT
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            return pd.to_datetime(s, format=fmt)
        except ValueError:
            continue
    return pd.NaT

date_cols = {
    "fbref_Naissance" : None,
    "espn_birthDate"  : None,
    "tsdb_dateBorn"   : None,
    "tm_DOB"          : None,
    "apif_birth_date" : None,
}

parsed = {col: recap[col].map(parse_date) for col in date_cols if col in recap.columns}

# Colonne consolidée : première valeur non nulle dans l'ordre de priorité
priority_dates = ["fbref_Naissance", "espn_birthDate", "tsdb_dateBorn", "tm_DOB", "apif_birth_date"]
recap["birth_date"] = pd.NaT
for col in priority_dates:
    if col in parsed:
        recap["birth_date"] = recap["birth_date"].combine_first(parsed[col])

recap["birth_date"] = pd.to_datetime(recap["birth_date"]).dt.date

filled = recap["birth_date"].notna().sum()
print(f"\n[birth_date] {filled} / {len(recap)} renseignés ({100*filled/len(recap):.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# 2. TAILLE (→ cm, float)
# ══════════════════════════════════════════════════════════════════════════════

def parse_height_cm(s):
    """Convertit n'importe quel format de taille en cm (float)."""
    if pd.isna(s):
        return np.nan
    s = str(s).replace("\xa0", " ").strip()

    # "178 cm" ou "186cm"
    m = re.search(r"(\d{2,3})\s*cm", s, re.I)
    if m:
        return float(m.group(1))

    # "1.86 m" ou "1,86 m" (mètres décimaux)
    m = re.search(r"(\d)[.,](\d{2})\s*m", s, re.I)
    if m:
        return round(float(f"{m.group(1)}.{m.group(2)}") * 100)

    # "6 ft 2 in" ou "6'2""
    m = re.search(r"(\d+)\s*ft\s*(\d+)\s*in", s, re.I)
    if m:
        return round(int(m.group(1)) * 30.48 + int(m.group(2)) * 2.54)

    # Valeur numérique brute
    try:
        v = float(s)
        if 1.5 <= v <= 2.2:   # en mètres
            return round(v * 100)
        if 140 <= v <= 220:    # déjà en cm
            return v
    except ValueError:
        pass

    return np.nan

# ESPN est en pouces — conversion directe
def inches_to_cm(v):
    try:
        v = float(v)
        return round(v * 2.54) if 55 <= v <= 90 else np.nan
    except (ValueError, TypeError):
        return np.nan

h_apif  = recap["apif_height"].map(parse_height_cm)        if "apif_height"   in recap.columns else pd.Series(np.nan, index=recap.index)
h_tsdb  = recap["tsdb_strHeight"].map(parse_height_cm)     if "tsdb_strHeight" in recap.columns else pd.Series(np.nan, index=recap.index)
h_tm    = recap["tm_Height (m)"].map(parse_height_cm)      if "tm_Height (m)" in recap.columns else pd.Series(np.nan, index=recap.index)
h_espn  = recap["espn_height_cm"].map(inches_to_cm)        if "espn_height_cm" in recap.columns else pd.Series(np.nan, index=recap.index)

# Priorité : tsdb_strHeight (format texte le plus riche) → tm → espn → apif
recap["height_cm"] = np.nan
for h in [h_tsdb, h_tm, h_espn, h_apif]:
    recap["height_cm"] = recap["height_cm"].combine_first(h)

# Validation : exclure les valeurs aberrantes
recap.loc[~recap["height_cm"].between(140, 220), "height_cm"] = np.nan

filled = recap["height_cm"].notna().sum()
print(f"[height_cm]  {filled} / {len(recap)} renseignés ({100*filled/len(recap):.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# 3. POIDS (→ kg, float)
# ══════════════════════════════════════════════════════════════════════════════

def parse_weight_kg(s):
    """Convertit n'importe quel format de poids en kg (float)."""
    if pd.isna(s):
        return np.nan
    s = str(s).strip()

    # "78 kg"
    m = re.search(r"(\d+)\s*kg", s, re.I)
    if m:
        return float(m.group(1))

    # "157 lbs"
    m = re.search(r"(\d+)\s*lbs?", s, re.I)
    if m:
        return round(float(m.group(1)) * 0.4536, 1)

    # Valeur numérique brute
    try:
        v = float(s)
        # ESPN : en livres (valeurs typiques 130–280 lbs = 59–127 kg)
        # On ne peut pas distinguer sans contexte → traité séparément
        return v if 40 <= v <= 130 else np.nan
    except ValueError:
        return np.nan

def lbs_to_kg(v):
    try:
        v = float(v)
        return round(v * 0.4536, 1) if 100 <= v <= 300 else np.nan
    except (ValueError, TypeError):
        return np.nan

w_apif = recap["apif_weight"].map(parse_weight_kg)     if "apif_weight"   in recap.columns else pd.Series(np.nan, index=recap.index)
w_tsdb = recap["tsdb_strWeight"].map(parse_weight_kg)  if "tsdb_strWeight" in recap.columns else pd.Series(np.nan, index=recap.index)
w_espn = recap["espn_weight_kg"].map(lbs_to_kg)        if "espn_weight_kg" in recap.columns else pd.Series(np.nan, index=recap.index)

# Priorité : apif (kg explicite) → tsdb_strWeight → espn converti
recap["weight_kg"] = np.nan
for w in [w_apif, w_tsdb, w_espn]:
    recap["weight_kg"] = recap["weight_kg"].combine_first(w)

recap.loc[~recap["weight_kg"].between(40, 130), "weight_kg"] = np.nan

filled = recap["weight_kg"].notna().sum()
print(f"[weight_kg]  {filled} / {len(recap)} renseignés ({100*filled/len(recap):.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# 4. NATIONALITÉ
# ══════════════════════════════════════════════════════════════════════════════

# Harmonisation des variantes connues
COUNTRY_MAP = {
    "the netherlands": "Netherlands",
    "holland":         "Netherlands",
    "ivory coast":     "Ivory Coast",
    "cote d'ivoire":   "Ivory Coast",
    "côte d'ivoire":   "Ivory Coast",
    "england":         "England",
    "scotland":        "Scotland",
    "wales":           "Wales",
    "northern ireland":"Northern Ireland",
    "usa":             "United States",
    "united states of america": "United States",
    "korea republic":  "South Korea",
    "republic of ireland": "Ireland",
}

def clean_nationality(s):
    if pd.isna(s):
        return np.nan
    s = str(s).strip()
    return COUNTRY_MAP.get(s.lower(), s)

# tm_Citizenship : liste Python sérialisée → on extrait le premier pays
def parse_citizenship(s):
    if pd.isna(s):
        return np.nan
    try:
        lst = ast.literal_eval(str(s))
        return lst[0] if lst else np.nan
    except (ValueError, SyntaxError):
        return np.nan

nat_tm    = recap["tm_Nationality"].map(clean_nationality)   if "tm_Nationality"     in recap.columns else pd.Series(np.nan, index=recap.index)
nat_espn  = recap["espn_nationality"].map(clean_nationality) if "espn_nationality"   in recap.columns else pd.Series(np.nan, index=recap.index)
nat_apif  = recap["apif_nationality"].map(clean_nationality) if "apif_nationality"   in recap.columns else pd.Series(np.nan, index=recap.index)
nat_tsdb  = recap["tsdb_strNationality"].map(clean_nationality) if "tsdb_strNationality" in recap.columns else pd.Series(np.nan, index=recap.index)
nat_cit   = recap["tm_Citizenship"].map(parse_citizenship).map(clean_nationality) if "tm_Citizenship" in recap.columns else pd.Series(np.nan, index=recap.index)

# Double nationalité : on garde tm_Citizenship (liste complète) comme colonne séparée
recap["nationality"] = np.nan
for nat in [nat_tm, nat_espn, nat_tsdb, nat_apif]:
    recap["nationality"] = recap["nationality"].combine_first(nat)

filled = recap["nationality"].notna().sum()
print(f"[nationality] {filled} / {len(recap)} renseignés ({100*filled/len(recap):.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# 5. POSITION
# ══════════════════════════════════════════════════════════════════════════════

# Table de correspondance → catégorie standard
POSITION_CAT = {
    # Gardien
    "goalkeeper": "Goalkeeper", "gk": "Goalkeeper",
    # Défenseur
    "defender": "Defender", "df": "Defender",
    "centre-back": "Defender", "cb": "Defender",
    "left-back": "Defender", "right-back": "Defender",
    "wing-back": "Defender",
    # Milieu
    "midfielder": "Midfielder", "mf": "Midfielder",
    "central midfield": "Midfielder",
    "defensive midfield": "Midfielder",
    "attacking midfield": "Midfielder",
    "defensive midfielder": "Midfielder",
    "central midfielder": "Midfielder",
    # Attaquant
    "attacker": "Attacker", "forward": "Attacker", "fw": "Attacker",
    "centre-forward": "Attacker", "left winger": "Attacker",
    "right winger": "Attacker", "second striker": "Attacker",
    "striker": "Attacker",
}

def fbref_to_cat(s):
    """Convertit les codes FBref (ex. 'DF-MF (FB)') en catégorie."""
    if pd.isna(s):
        return np.nan
    s = str(s).upper()
    if s.startswith("GK"):   return "Goalkeeper"
    if s.startswith("DF"):   return "Defender"
    if s.startswith("MF"):   return "Midfielder"
    if s.startswith("FW"):   return "Attacker"
    return np.nan

def to_cat(s):
    if pd.isna(s):
        return np.nan
    return POSITION_CAT.get(str(s).lower().strip(), np.nan)

pos_tm    = recap["tm_Position"].map(to_cat)       if "tm_Position"    in recap.columns else pd.Series(np.nan, index=recap.index)
pos_tsdb  = recap["tsdb_strPosition"].map(to_cat)  if "tsdb_strPosition" in recap.columns else pd.Series(np.nan, index=recap.index)
pos_espn  = recap["espn_position"].map(to_cat)     if "espn_position"  in recap.columns else pd.Series(np.nan, index=recap.index)
pos_apif  = recap["apif_position"].map(to_cat)     if "apif_position"  in recap.columns else pd.Series(np.nan, index=recap.index)
pos_fbref = recap["fbref_Position"].map(fbref_to_cat) if "fbref_Position" in recap.columns else pd.Series(np.nan, index=recap.index)
pos_apife = recap["apife_player_pos"].map(to_cat)  if "apife_player_pos" in recap.columns else pd.Series(np.nan, index=recap.index)

# Sous-position détaillée : TM en priorité (vocabulaire le plus précis)
recap["position"] = np.nan
for pos in [pos_tm, pos_tsdb, pos_espn, pos_fbref, pos_apif, pos_apife]:
    recap["position"] = recap["position"].combine_first(pos)

recap["position_detail"] = np.nan
for col in ["tm_Position", "tsdb_strPosition", "espn_position"]:
    if col in recap.columns:
        recap["position_detail"] = recap["position_detail"].combine_first(recap[col])

filled = recap["position"].notna().sum()
print(f"[position]   {filled} / {len(recap)} renseignés ({100*filled/len(recap):.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# 6. CLUB ACTUEL
# ══════════════════════════════════════════════════════════════════════════════

# TM est la référence (meilleure couverture, noms cohérents)
recap["club"] = np.nan
for col in ["tm_Team", "apife_team_name", "espn_club", "fbref_Club"]:
    if col in recap.columns:
        recap["club"] = recap["club"].combine_first(recap[col])

filled = recap["club"].notna().sum()
print(f"[club]       {filled} / {len(recap)} renseignés ({100*filled/len(recap):.1f}%)")

# ══════════════════════════════════════════════════════════════════════════════
# 7. RÉORGANISATION ET EXPORT
# ══════════════════════════════════════════════════════════════════════════════

# Colonnes consolidées insérées juste après les colonnes sources correspondantes
consolidated = ["birth_date", "height_cm", "weight_kg", "nationality", "position", "position_detail", "club"]

# On les place en tête pour visibilité
existing_cols = [c for c in recap.columns if c not in consolidated]
recap = recap[consolidated + existing_cols]

recap.to_csv(DATA_DIR / "recap_joueurs_clean.csv", index=False, encoding="utf-8-sig")
print(f"\nFichier exporté : {recap.shape[0]} lignes × {recap.shape[1]} colonnes")

# Vérification finale
print("\n── Taux de remplissage colonnes consolidées ──")
for col in consolidated:
    n = recap[col].notna().sum()
    print(f"  {col:<20} {n:>6} / {len(recap)}  ({100*n/len(recap):.1f}%)")