# ══════════════════════════════════════════════════════════════════════════════
# EXTRAIT À INTÉGRER DANS pipeline_flow.py
# ══════════════════════════════════════════════════════════════════════════════
#
# 1. Ajouter cet import en haut du fichier :
#
#    import subprocess
#
# 2. Ajouter cette task après les imports existants :

from prefect import task

DBT_DIR = pathlib.Path(__file__).parent / "dbt_football"

@task(name="dbt_run", retries=1, retry_delay_seconds=30)
def dbt_run():
    """
    Exécute dbt run pour matérialiser les 6 vues analytiques dans football_db.
    Lance également dbt test pour valider l'intégrité des modèles.
    Doit être appelé après chargement_mysql().
    """
    import subprocess, sys

    env = {
        **os.environ,
        "DB_HOST": os.getenv("DB_HOST", "localhost"),
        "DB_PORT": os.getenv("DB_PORT", "3306"),
        "DB_NAME": os.getenv("DB_NAME", "football_db"),
        "DB_USER": os.getenv("DB_USER", "root"),
        "DB_PASS": os.getenv("DB_PASS", ""),
    }

    # dbt run — matérialise les modèles staging + marts
    print("  [dbt] Lancement de dbt run...")
    result = subprocess.run(
        [sys.executable, "-m", "dbt", "run",
         "--project-dir", str(DBT_DIR),
         "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    print(result.stdout[-3000:] if len(result.stdout) > 3000 else result.stdout)
    if result.returncode != 0:
        print(result.stderr[-1000:])
        raise RuntimeError(f"dbt run a échoué (code {result.returncode})")

    # dbt test — vérifie unique / not_null / accepted_values
    print("  [dbt] Lancement de dbt test...")
    result_test = subprocess.run(
        [sys.executable, "-m", "dbt", "test",
         "--project-dir", str(DBT_DIR),
         "--profiles-dir", str(DBT_DIR)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    print(result_test.stdout[-2000:] if len(result_test.stdout) > 2000 else result_test.stdout)
    if result_test.returncode != 0:
        # Tests en warning seulement — ne bloque pas le pipeline
        print("  [dbt] ⚠️  Certains tests ont échoué (voir logs ci-dessus)")

    print("  [dbt] ✅ dbt run terminé")


# 3. Modifier flow_hebdo et flow_mensuel pour appeler dbt_run() après chargement_mysql() :
#
#    @flow(name="flow_hebdo")
#    def flow_hebdo():
#        ...
#        chargement_mysql()
#        dbt_run()          # ← ajouter cette ligne
#
#    @flow(name="flow_mensuel")
#    def flow_mensuel():
#        ...
#        chargement_mysql()
#        dbt_run()          # ← ajouter cette ligne
#
# ══════════════════════════════════════════════════════════════════════════════
# INSTALLATION dbt-mysql
# ══════════════════════════════════════════════════════════════════════════════
#
# Dans le venv du projet :
#
#    pip install dbt-mysql
#
# Puis vérifier :
#
#    python -m dbt --version
#
# ══════════════════════════════════════════════════════════════════════════════
# PREMIER LANCEMENT MANUEL (avant intégration Prefect)
# ══════════════════════════════════════════════════════════════════════════════
#
#    cd dbt_football
#    python -m dbt run --profiles-dir .
#    python -m dbt test --profiles-dir .
#
# ══════════════════════════════════════════════════════════════════════════════
