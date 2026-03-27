import pandas as pd
import unicodedata, re
import pathlib
DATA_DIR = pathlib.Path(__file__).parent / "data"

# ── Helpers ────────────────────────────────────────────────────────────────────

def normalize(s):
    if pd.isna(s):
        return None
    s = str(s).lower().strip()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = re.sub(r"[-'\u2019]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def add_key(df, col):
    df = df.copy()
    df["_key"] = df[col].map(normalize)
    return df

def prefix_cols(df, prefix):
    return df.rename(columns={c: f"{prefix}_{c}" for c in df.columns if c != "_key"})

def dedup(df, label):
    n = df["_key"].duplicated().sum()
    if n:
        print(f"  ⚠ {label} : {n} doublons _key supprimés")
    return df.drop_duplicates(subset="_key").reset_index(drop=True)

# ── Chargement ─────────────────────────────────────────────────────────────────

apif  = pd.read_csv(DATA_DIR / "API_F_Joueurs.csv",                       low_memory=False)
fbref = pd.read_csv(DATA_DIR / "FBref_Joueurs.csv",                       low_memory=False)
equip = pd.read_csv(DATA_DIR / "API_F_Equipes.csv",                       low_memory=False)
espn  = pd.read_csv(DATA_DIR / "ESPN_AF_stats.csv",                       low_memory=False)
tm    = pd.read_csv(DATA_DIR / "players_all.csv",                         low_memory=False)
enr   = pd.read_csv(DATA_DIR / "players_enriched.csv",                    low_memory=False)
tsdb  = pd.read_csv(DATA_DIR / "TheSportsDB_joueurs_top20_fifa_2026.csv", low_memory=False)

# ── Clés de normalisation ──────────────────────────────────────────────────────

apif["_key"] = apif.apply(
    lambda r: normalize(f"{r['firstname']} {r['lastname']}")
              if pd.notna(r["firstname"]) and pd.notna(r["lastname"])
              else normalize(r["name"]),
    axis=1
)

fbref  = add_key(fbref,  "Nom")
espn   = add_key(espn,   "name")
tm     = add_key(tm,     "Name")
enr    = add_key(enr,    "Name")

equip  = equip.sort_values("player_id").drop_duplicates(subset="player_id")
equip  = add_key(equip, "player_name")
equip["_truncated"] = equip["player_name"].str.match(r"^[A-ZÀ-Ö]\.\s", na=False)

tsdb["_key"]  = tsdb["strPlayer"].map(normalize)
tsdb["_key2"] = tsdb["strPlayerAlternate"].map(normalize)

# ── Index global des joueurs ───────────────────────────────────────────────────

all_keys = set()
for df, col in [
    (apif, "_key"), (fbref, "_key"), (espn, "_key"),
    (tm,   "_key"), (enr,   "_key"), (tsdb, "_key"),
    (tsdb, "_key2"),
]:
    all_keys.update(df[col].dropna().unique())

recap = pd.DataFrame({"_key": sorted(all_keys)})
print(f"Joueurs uniques (clés normalisées) : {len(recap)}")

# ── Préfixage ──────────────────────────────────────────────────────────────────

apif_p  = dedup(prefix_cols(apif,  "apif"),  "apif")
fbref_p = dedup(prefix_cols(fbref, "fbref"), "fbref")
equip_p = dedup(prefix_cols(equip, "apife"), "apife")
espn_p  = dedup(prefix_cols(espn,  "espn"),  "espn")

tm_p   = prefix_cols(tm,  "tm")
enr_p  = prefix_cols(enr, "enr")

tm_p_d  = tm_p.drop_duplicates(subset="tm_ID").reset_index(drop=True)
enr_p_d = (enr_p
            .drop(columns=[c for c in ["enr_Name", "enr_league"] if c in enr_p.columns])
            .drop_duplicates(subset="enr_ID"))

tm_keys = tm_p_d["_key"].copy()
tm_enr  = tm_p_d.merge(enr_p_d, left_on="tm_ID", right_on="enr_ID", how="left").reset_index(drop=True)
tm_enr["_key"] = tm_keys.values
tm_enr = dedup(tm_enr, "tm+enr")

tsdb_cols_orig = [c for c in tsdb.columns if c not in ("_key", "_key2")]
tsdb_p = tsdb.rename(columns={c: f"tsdb_{c}" for c in tsdb_cols_orig})

tsdb_main = dedup(tsdb_p.drop(columns=["_key2"]), "tsdb_main")
tsdb_alt  = (tsdb_p.dropna(subset=["_key2"])
              .drop_duplicates(subset="_key2")
              .drop(columns=["_key"])
              .rename(columns={"_key2": "_key"}))
tsdb_alt  = dedup(tsdb_alt, "tsdb_alt")

# ── Merges ─────────────────────────────────────────────────────────────────────

recap = recap.merge(apif_p,    on="_key", how="left")
recap = recap.merge(fbref_p,   on="_key", how="left")
recap = recap.merge(equip_p,   on="_key", how="left")
recap = recap.merge(espn_p,    on="_key", how="left")
recap = recap.merge(tm_enr,    on="_key", how="left")
recap = recap.merge(tsdb_main, on="_key", how="left")

# Tentative via strPlayerAlternate pour les lignes sans match TheSportsDB
tsdb_id_col = "tsdb_idPlayer"
no_tsdb = recap[tsdb_id_col].isna()
print(f"Lignes sans match TheSportsDB (clé principale) : {no_tsdb.sum()}")
if no_tsdb.any():
    tsdb_fill_cols = [c for c in tsdb_p.columns if c.startswith("tsdb_")]
    fill = recap.loc[no_tsdb, ["_key"]].merge(tsdb_alt, on="_key", how="left")
    recap.loc[no_tsdb, tsdb_fill_cols] = fill[tsdb_fill_cols].values

# ── Second passage apife : résolution des noms tronqués ───────────────────────

no_apife = recap["apife_player_id"].isna()
print(f"Lignes sans match apife (avant résolution tronqués) : {no_apife.sum()}")

trunc = equip[equip["_truncated"]].copy()
trunc["_init"]     = trunc["player_name"].str[0].str.lower()
trunc["_lastname"] = trunc["player_name"].str.split(". ").str[1].map(normalize)

known_keys = set(recap["_key"].dropna())

def resolve_truncated(init, lastname, known_keys):
    if pd.isna(lastname):
        return None
    candidates = [k for k in known_keys if k and k.endswith(lastname) and k.startswith(init)]
    return candidates[0] if len(candidates) == 1 else None

trunc["_resolved_key"] = trunc.apply(
    lambda r: resolve_truncated(r["_init"], r["_lastname"], known_keys), axis=1
)

resolved = trunc.dropna(subset=["_resolved_key"])
print(f"Noms tronqués apife résolus : {len(resolved)} / {len(trunc)}")

equip_resolved = (
    prefix_cols(resolved.drop(columns=["_key", "_truncated", "_init", "_lastname"]), "apife")
    .rename(columns={"apife__resolved_key": "_key"})
    .drop_duplicates(subset="_key")
)
apife_fill_cols = [c for c in equip_resolved.columns if c.startswith("apife_")]

no_apife_idx = recap.index[recap["apife_player_id"].isna()]
fill2 = recap.loc[no_apife_idx, ["_key"]].merge(equip_resolved, on="_key", how="left")
for col in apife_fill_cols:
    if col in fill2.columns and col in recap.columns:
        recap.loc[no_apife_idx, col] = fill2[col].values

no_apife_after = recap["apife_player_id"].isna()
print(f"Lignes sans match apife (après résolution tronqués)  : {no_apife_after.sum()}")

print(f"\nLignes recap : {len(recap)}  |  _key uniques : {recap['_key'].nunique()}")
dup_n = recap["_key"].duplicated().sum()
print(f"Doublons sur _key : {dup_n} {'✅' if dup_n == 0 else '⚠'}")

# ── Réorganisation des colonnes ────────────────────────────────────────────────

identity_cols = [
    # Noms
    "apif_name", "apif_firstname", "apif_lastname",
    "fbref_Nom", "apife_player_name", "espn_name", "tm_Name", "enr_Name",
    "tsdb_strPlayer", "tsdb_strPlayerAlternate",
    "espn_short_name",
    # Dates de naissance
    "apif_birth_date", "apif_birth_place", "apif_birth_country",
    "fbref_Naissance", "espn_birth_date", "tm_DOB", "tsdb_dateBorn",
    "tsdb_strBirthLocation",
    # Âge
    "apif_age", "espn_age", "tm_Age", "tsdb_age",
    # Nationalité
    "apif_nationality", "espn_nationality", "tm_Nationality", "tm_Citizenship",
    "tsdb_strNationality",
    # Morphologie
    "apif_height", "apif_weight",
    "espn_height_cm", "espn_weight_kg",
    "tm_Height (m)", "tsdb_strHeight", "tsdb_strWeight",
    "tsdb_height_m", "tsdb_weight_kg",
    # Position
    "apif_position", "fbref_Position", "apife_player_pos", "espn_position",
    "tm_Position", "tm_Other positions", "tsdb_strPosition",
    # Club & ligue
    "fbref_Club", "apife_team_name", "espn_club", "espn_league",
    "tm_Team", "tm_league",
    # Identifiants
    "apif_id", "apife_player_id", "espn_espn_id", "tm_ID", "enr_ID",
    "tsdb_idPlayer", "tsdb_idAPIfootball",
    # Traçabilité stats
    "espn_stats_source",
]

remaining = [c for c in recap.columns if c not in identity_cols and c != "_key"]
final_cols = [c for c in identity_cols if c in recap.columns] + remaining
recap = recap[final_cols]

# ── Export ─────────────────────────────────────────────────────────────────────

recap.to_csv(DATA_DIR / "recap_joueurs.csv", index=False, encoding="utf-8-sig")
print(f"\nFichier exporté : {recap.shape[0]} lignes × {recap.shape[1]} colonnes")
id_preview = [c for c in identity_cols if c in recap.columns][:10]
print(recap[id_preview].head(10).to_string())