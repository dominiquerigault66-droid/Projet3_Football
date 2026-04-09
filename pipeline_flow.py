"""
pipeline_flow.py — Orchestration Prefect du pipeline Football
=============================================================
Deux flows disponibles :

  flow_hebdo   — API-Football joueurs+équipes, ESPN+AF stats, TheSportsDB
  flow_mensuel — toutes sources (hebdo + Transfermarkt + Capology/Sofascore)

Les deux flows enchaînent :
  collecte(s) -> merge -> nettoyage -> chargement MySQL

Lancement manuel (test) :
  python pipeline_flow.py --flow hebdo
  python pipeline_flow.py --flow mensuel

Lancement d'une task seule : modifier le bloc if __name__ en bas du fichier.
"""

import os
import argparse
import subprocess
from pathlib import Path

from prefect import flow, task

ROOT = Path(__file__).parent

# ── Utilitaire : exécution d'un script enfant ─────────────────────────────────

def run_script(script_name: str):
    """Lance un script Python du projet avec le venv courant, en UTF-8."""
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"

    result = subprocess.run(
        [str(ROOT / ".venv" / "Scripts" / "python.exe"), str(ROOT / script_name)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"{script_name} a échoué (code {result.returncode})")

# ── Tasks collecte — sources hebdomadaires ────────────────────────────────────

@task(name="Collecte API-Football joueurs", retries=2, retry_delay_seconds=60)
def collecte_apif_joueurs():
    run_script("Collecte_API_Football_joueurs.py")

@task(name="Collecte API-Football equipes", retries=2, retry_delay_seconds=60)
def collecte_apif_teams():
    run_script("Collecte_API-FB_team.py")

@task(name="Collecte ESPN + AF stats", retries=2, retry_delay_seconds=30)
def collecte_espn_af():
    run_script("Collecte_ESPN_AF_stats.py")

@task(name="Collecte TheSportsDB", retries=3, retry_delay_seconds=20)
def collecte_thesportsdb():
    run_script("Collecte_TheSportsDB.py")

# ── Tasks collecte — sources mensuelles ───────────────────────────────────────

@task(name="Collecte Transfermarkt", retries=2, retry_delay_seconds=120)
def collecte_transfermarkt():
    run_script("Collecte_monScraperFC.py")

@task(name="Collecte Capology + Sofascore", retries=2, retry_delay_seconds=120)
def collecte_enriched():
    run_script("Collecte_monScraperFC_enriched.py")

# ── Tasks traitement ──────────────────────────────────────────────────────────

@task(name="Merge sources -> recap_joueurs.csv")
def merge_joueurs():
    run_script("Merge_joueurs.py")

@task(name="Nettoyage -> recap_joueurs_clean.csv")
def nettoyage_joueurs():
    run_script("Nettoyage_joueurs.py")

@task(name="Chargement MySQL", retries=2, retry_delay_seconds=30)
def chargement_mysql():
    run_script("import_mysql.py")

# ── Flow hebdomadaire ─────────────────────────────────────────────────────────
# Exécution séquentielle (compatible Prefect 3 sans task runner asyncio) :
#   apif_joueurs → apif_teams → espn_af → thesportsdb → merge → nettoyage → mysql

@flow(name="Pipeline Football - Hebdomadaire")
def flow_hebdo():
    collecte_apif_joueurs()
    collecte_apif_teams()
    collecte_espn_af()
    collecte_thesportsdb()
    merge_joueurs()
    nettoyage_joueurs()
    chargement_mysql()

# ── Flow mensuel ──────────────────────────────────────────────────────────────
# Exécution séquentielle :
#   apif_joueurs → apif_teams → espn_af → thesportsdb
#   → transfermarkt → enriched (lit players_all.csv)
#   → merge → nettoyage → mysql

@flow(name="Pipeline Football - Mensuel")
def flow_mensuel():
    collecte_apif_joueurs()
    collecte_apif_teams()
    collecte_espn_af()
    collecte_thesportsdb()
    collecte_transfermarkt()
    collecte_enriched()
    merge_joueurs()
    nettoyage_joueurs()
    chargement_mysql()

# ── Point d'entrée ────────────────────────────────────────────────────────────
#
# Pour tester une task seule, remplacer le contenu du bloc par ex. :
#   collecte_apif_joueurs()
#   collecte_espn_af()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pipeline Football - Prefect")
    parser.add_argument(
        "--flow",
        choices=["hebdo", "mensuel"],
        default="mensuel",
        help="Flow a executer (defaut : mensuel)",
    )
    args = parser.parse_args()

    if args.flow == "hebdo":
        print("Lancement : flow_hebdo")
        flow_hebdo()
    else:
        print("Lancement : flow_mensuel")
        flow_mensuel()
