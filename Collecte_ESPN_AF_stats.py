"""
Collecte_ESPN_AF_stats.py
═══════════════════════════════════════════════════════════════════
Complément aux 6 scripts existants — apporte UNIQUEMENT ce qui
n'est pas déjà couvert :

  1. Effectifs ESPN  (équipes + rosters par ligue)
  2. Stats de performance API-Football par ligue en cache
     (/players?league=&season= — jamais de requête individuelle)
  3. Complétion ESPN stats endpoint pour les joueurs sans match AF
  4. Export final → data/ESPN_AF_stats.csv

Ce script est indépendant : il produit son propre CSV qui sera
joint à recap_joueurs.csv via Merge_joueurs.py sur player_id / name.
═══════════════════════════════════════════════════════════════════
"""

import os
import json
import time
import pathlib
import requests
import pandas as pd
from dotenv import load_dotenv

# ── Config ─────────────────────────────────────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("API_FOOTBALL_KEY")

DATA_DIR     = pathlib.Path(__file__).parent / "data"
OUTPUT_FILE  = DATA_DIR / "ESPN_AF_stats.csv"
BACKUP_FILE  = DATA_DIR / "ESPN_AF_stats_backup.json"

ESPN_BASE    = "https://site.api.espn.com/apis/site/v2/sports/soccer"
ESPN_STATS   = "https://site.web.api.espn.com/apis/common/v3/sports/soccer"
AF_URL       = "https://v3.football.api-sports.io"

HEADERS_ESPN = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}
HEADERS_AF = {
    "x-apisports-key": API_KEY,
}

# Ligues couvertes : ESPN slug → {nom, AF league_id, saison}
LEAGUES = {
    "eng.1":                 {"name": "Premier League",   "af_id": 39,  "season": 2024},
    "esp.1":                 {"name": "La Liga",           "af_id": 140, "season": 2024},
    "ita.1":                 {"name": "Serie A",           "af_id": 135, "season": 2024},
    "ger.1":                 {"name": "Bundesliga",        "af_id": 78,  "season": 2024},
    "fra.1":                 {"name": "Ligue 1",           "af_id": 61,  "season": 2024},
    "por.1":                 {"name": "Primeira Liga",     "af_id": 94,  "season": 2024},
    "uefa.champions_league": {"name": "Champions League",  "af_id": 2,   "season": 2024},
}

# ── Helpers ────────────────────────────────────────────────────────────────────
def safe_get(url, headers, params=None, retries=3, delay=3):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=12)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait = delay * (attempt + 1) * 2
                print(f"    ⚠️  Rate limit — pause {wait}s")
                time.sleep(wait)
            elif r.status_code == 404:
                return None
            else:
                print(f"    ❌ HTTP {r.status_code} — {url}")
                return None
        except requests.RequestException as e:
            print(f"    ❌ Erreur réseau : {e}")
            time.sleep(5)
    return None

def to_int(v):
    try:
        return int(float(v)) if v is not None else None
    except (ValueError, TypeError):
        return None

def to_float(v):
    try:
        return float(v) if v is not None else None
    except (ValueError, TypeError):
        return None


# ══════════════════════════════════════════════════════════════════
# PHASE 1 — Effectifs ESPN
# Récupère equipes + rosters ; stats laissées à None (remplies ph.2/3)
# ══════════════════════════════════════════════════════════════════
def collect_espn_rosters() -> list[dict]:
    """Retourne la liste brute des joueurs collectés via ESPN."""
    all_players = []
    seen_ids    = set()

    for league_slug, league_info in LEAGUES.items():
        print(f"\n🏆 {league_info['name']} — effectifs ESPN")

        # Récupération des équipes
        data = safe_get(f"{ESPN_BASE}/{league_slug}/teams?limit=100", HEADERS_ESPN)
        if not data:
            print(f"   ⚠️  Aucune équipe récupérée")
            continue
        teams = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        print(f"   {len(teams)} équipes")
        time.sleep(2)

        for i, team_entry in enumerate(teams, 1):
            team      = team_entry.get("team", {})
            team_id   = team.get("id")
            team_name = team.get("displayName", "")

            roster_data = safe_get(
                f"{ESPN_BASE}/{league_slug}/teams/{team_id}/roster",
                HEADERS_ESPN
            )
            if not roster_data:
                time.sleep(1)
                continue

            novos = 0
            for p in roster_data.get("athletes", []):
                pid = p.get("id")
                if pid in seen_ids:
                    continue
                seen_ids.add(pid)
                all_players.append({
                    # Identifiants
                    "espn_id":       pid,
                    "league_slug":   league_slug,
                    # Profil
                    "name":          p.get("fullName"),
                    "short_name":    p.get("shortName"),
                    "birth_date":    (p.get("dateOfBirth") or "")[:10] or None,
                    "age":           p.get("age"),
                    "nationality":   p.get("citizenship"),
                    "height_cm":     p.get("height"),
                    "weight_kg":     p.get("weight"),
                    "photo":         (p.get("headshot") or {}).get("href"),
                    "position":      (p.get("position") or {}).get("displayName"),
                    "jersey_number": p.get("jersey"),
                    "club":          team_name,
                    "league":        league_info["name"],
                    # Stats — remplies phases 2 & 3
                    "goals":         None, "assists":       None,
                    "appearances":   None, "minutes_played": None,
                    "yellow_cards":  None, "red_cards":     None,
                    "shots":         None, "shots_on_target": None,
                    "tackles":       None, "fouls":         None,
                    "passes":        None, "pass_accuracy": None,
                    "saves":         None, "clean_sheets":  None,
                    "rating":        None,
                    # Source
                    "stats_source":  None,
                })
                novos += 1

            print(f"   [{i}/{len(teams)}] {team_name} — {novos} nouveaux | total : {len(all_players)}")
            time.sleep(2)

    print(f"\n✅ Phase 1 terminée — {len(all_players)} joueurs")
    return all_players


# ══════════════════════════════════════════════════════════════════
# PHASE 2 — Stats API-Football (chargement en cache par ligue)
# Jamais de requête individuelle : une seule passe par ligue.
# ══════════════════════════════════════════════════════════════════
_af_cache: dict[str, dict] = {}   # league_slug → {name_lower: stats_dict}

def _build_af_cache(league_slug: str):
    """Charge toutes les stats d'une ligue en mémoire (pagination complète)."""
    if league_slug in _af_cache:
        return

    info    = LEAGUES[league_slug]
    cache   = {}
    page    = 1

    print(f"\n   📡 Cache API-Football — {info['name']}...")

    while True:
        data = safe_get(
            f"{AF_URL}/players",
            HEADERS_AF,
            params={"league": info["af_id"], "season": info["season"], "page": page},
            retries=3, delay=5,
        )
        if not data:
            break

        entries = data.get("response", [])
        if not entries:
            break

        for entry in entries:
            p    = entry.get("player", {})
            st   = (entry.get("statistics") or [{}])[0]
            name = (p.get("name") or "").lower().strip()
            if not name:
                continue

            gm = st.get("games",      {}) or {}
            gl = st.get("goals",      {}) or {}
            cd = st.get("cards",      {}) or {}
            sh = st.get("shots",      {}) or {}
            tk = st.get("tackles",    {}) or {}
            fo = st.get("fouls",      {}) or {}
            ps = st.get("passes",     {}) or {}
            gk = st.get("goalkeeper", {}) or {}

            cache[name] = {
                "goals":          to_int(gl.get("total")),
                "assists":        to_int(gl.get("assists")),
                "appearances":    to_int(gm.get("appearences")),  # typo API conservée
                "minutes_played": to_int(gm.get("minutes")),
                "yellow_cards":   to_int(cd.get("yellow")),
                "red_cards":      to_int(cd.get("red")),
                "shots":          to_int(sh.get("total")),
                "shots_on_target":to_int(sh.get("on")),
                "tackles":        to_int(tk.get("total")),
                "fouls":          to_int(fo.get("committed")),
                "passes":         to_int(ps.get("total")),
                "pass_accuracy":  to_float(ps.get("accuracy")),
                "saves":          to_int(gk.get("saves")),
                "clean_sheets":   to_int(gk.get("clean_sheet"))
                                  if isinstance(gk.get("clean_sheet"), int) else None,
                "rating":         to_float(gm.get("rating")),
            }

        paging  = data.get("paging", {})
        current = paging.get("current", 1)
        total   = paging.get("total",   1)
        print(f"      Page {current}/{total} — {len(cache)} joueurs en cache")

        if current >= total:
            break
        page += 1
        time.sleep(2)

    _af_cache[league_slug] = cache
    print(f"   ✅ Cache prêt : {len(cache)} joueurs")


def _lookup_af(name: str, league_slug: str) -> dict | None:
    """
    Cherche un joueur dans le cache AF.
    Stratégie : correspondance exacte → dernier mot → premier mot.
    Retourne None si aucun match (pas de requête individuelle).
    """
    if not name:
        return None
    _build_af_cache(league_slug)
    cache = _af_cache.get(league_slug, {})
    nl    = name.lower().strip()

    if nl in cache:
        return cache[nl]

    parts = nl.split()
    if not parts:
        return None

    # Dernière partie (nom de famille le plus souvent)
    candidates = [s for k, s in cache.items() if parts[-1] in k.split()]
    if len(candidates) == 1:
        return candidates[0]

    # Première partie (prénom)
    candidates = [s for k, s in cache.items() if parts[0] in k.split()]
    if len(candidates) == 1:
        return candidates[0]

    return None


def enrich_with_af(players: list[dict]) -> tuple[int, int]:
    """Enrichit les joueurs sans stats via le cache API-Football."""
    found = empty = 0

    # Pré-charger tous les caches nécessaires d'un coup
    slugs_needed = {p["league_slug"] for p in players}
    for slug in slugs_needed:
        _build_af_cache(slug)

    for p in players:
        if _has_stats(p):
            continue
        stats = _lookup_af(p["name"], p["league_slug"])
        if stats:
            p.update(stats)
            p["stats_source"] = "api_football"
            found += 1
        else:
            empty += 1

    print(f"   ✅ API-Football : {found} enrichis | {empty} sans correspondance")
    return found, empty


# ══════════════════════════════════════════════════════════════════
# PHASE 3 — Complétion ESPN stats endpoint
# Uniquement pour les joueurs encore sans stats après phase 2.
# ══════════════════════════════════════════════════════════════════
def _fetch_espn_stats(espn_id: str, league_slug: str) -> dict | None:
    """Récupère les stats d'un joueur via l'endpoint ESPN stats."""
    data = safe_get(
        f"{ESPN_STATS}/{league_slug}/athletes/{espn_id}/stats",
        HEADERS_ESPN,
    )
    if not data:
        return None

    raw = {}
    for cat in (data.get("splits") or {}).get("categories", []):
        for s in cat.get("stats", []):
            n = s.get("name")
            v = s.get("value")
            if n:
                raw[n] = v

    if not raw:
        return None

    result = {
        "goals":          to_int(raw.get("goals")),
        "assists":        to_int(raw.get("goalAssists")),
        "appearances":    to_int(raw.get("gamesPlayed")),
        "minutes_played": to_int(raw.get("minutesPlayed")),
        "yellow_cards":   to_int(raw.get("yellowCards")),
        "red_cards":      to_int(raw.get("redCards")),
        "shots":          to_int(raw.get("totalShots")),
        "shots_on_target":to_int(raw.get("shotsOnTarget")),
        "saves":          to_int(raw.get("saves")),
    }
    return result if any(v is not None for v in result.values()) else None


def enrich_with_espn_stats(players: list[dict]) -> tuple[int, int]:
    """Appelle ESPN stats pour les joueurs encore sans stats."""
    targets = [p for p in players if not _has_stats(p)]
    found = empty = 0

    print(f"\n   📡 ESPN stats — {len(targets)} joueurs à compléter")

    for i, p in enumerate(targets, 1):
        stats = _fetch_espn_stats(p["espn_id"], p["league_slug"])
        if stats:
            p.update(stats)
            p["stats_source"] = "espn_stats"
            found += 1
            print(f"   [{i}/{len(targets)}] ✅ {p['name']} — goals={p.get('goals')} rating={p.get('rating')}")
        else:
            empty += 1
            print(f"   [{i}/{len(targets)}] ⚠️  {p['name']} — sans stats ESPN")
        time.sleep(1)

    print(f"   ✅ ESPN stats : {found} enrichis | {empty} sans données")
    return found, empty


# ── Utilitaire ─────────────────────────────────────────────────────────────────
def _has_stats(p: dict) -> bool:
    return any(
        p.get(f) is not None
        for f in ["goals", "assists", "appearances", "minutes_played",
                  "yellow_cards", "red_cards", "shots", "rating"]
    )


# ══════════════════════════════════════════════════════════════════
# DIAGNOSTICS
# ══════════════════════════════════════════════════════════════════
def diagnostique_af():
    data = safe_get(f"{AF_URL}/status", HEADERS_AF)
    if data and isinstance(data.get("response"), dict):
        req = data["response"].get("requests", {})
        sub = data["response"].get("subscription", {})
        print(f"  ✅ API-Football OK | Plan : {sub.get('plan','?')}")
        print(f"  Requêtes aujourd'hui : {req.get('current','?')}/{req.get('limit_day','?')}")
    else:
        print("  ❌ API-Football inaccessible — vérifier API_FOOTBALL_KEY dans .env")


def print_fill_rates(players: list[dict]):
    fields = ["goals","assists","appearances","minutes_played","yellow_cards",
              "red_cards","shots","shots_on_target","tackles","fouls",
              "passes","pass_accuracy","saves","clean_sheets","rating","photo"]
    total  = len(players)
    print(f"\n{'─'*52}")
    print(f"{'Champ':<20} {'Remplis':>8} {'%':>7}")
    print(f"{'─'*52}")
    for f in fields:
        n   = sum(1 for p in players if p.get(f) is not None)
        pct = n / total * 100 if total else 0
        bar = "█" * int(pct / 5)
        print(f"  {f:<18} {n:>6}/{total}  {pct:5.1f}%  {bar}")
    print(f"{'─'*52}")

    # Sources
    sources = {}
    for p in players:
        s = p.get("stats_source") or "aucune"
        sources[s] = sources.get(s, 0) + 1
    print("\nSources des stats :")
    for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
        print(f"  {src:<20} {cnt:>5} joueurs")


# ══════════════════════════════════════════════════════════════════
# EXÉCUTION PRINCIPALE
# ══════════════════════════════════════════════════════════════════
if __name__ == "__main__":

    print("\n" + "═" * 55)
    print("  COLLECTE ESPN + API-FOOTBALL STATS")
    print("═" * 55)

    # Diagnostic
    print("\n🔬 Diagnostic API-Football...")
    diagnostique_af()

    # ── Phase 1 : effectifs ESPN ──────────────────────────────────
    print("\n🚀 PHASE 1 — Effectifs ESPN\n")
    players = collect_espn_rosters()

    # Backup JSON après phase 1
    with open(BACKUP_FILE, "w", encoding="utf-8") as f:
        json.dump(players, f, ensure_ascii=False, indent=2)
    print(f"   💾 Backup phase 1 → {BACKUP_FILE}")

    # ── Phase 2 : stats API-Football (cache par ligue) ────────────
    print("\n🚀 PHASE 2 — Stats API-Football (cache par ligue)\n")
    enrich_with_af(players)

    # Backup JSON après phase 2
    with open(BACKUP_FILE, "w", encoding="utf-8") as f:
        json.dump(players, f, ensure_ascii=False, indent=2)
    print(f"   💾 Backup phase 2 → {BACKUP_FILE}")

    # ── Phase 3 : complétion ESPN stats ──────────────────────────
    print("\n🚀 PHASE 3 — Complétion ESPN stats endpoint\n")
    enrich_with_espn_stats(players)

    # ── Export CSV ────────────────────────────────────────────────
    df = pd.DataFrame(players)

    # Suppression de la colonne interne league_slug (non utile en aval)
    df = df.drop(columns=["league_slug"], errors="ignore")

    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    # ── Rapport final ─────────────────────────────────────────────
    print(f"\n{'═'*55}")
    print(f"  RÉSULTAT FINAL")
    print(f"{'═'*55}")
    print(f"  Joueurs collectés : {len(df)}")
    print(f"  Colonnes          : {len(df.columns)}")
    print(f"  Fichier           : {OUTPUT_FILE}")
    print_fill_rates(players)
    print(f"\n🎉 Terminé !\n")