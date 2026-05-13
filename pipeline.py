import subprocess
import json
from datetime import datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent


def run_step(step_name, script_name):
    print("=" * 70)
    print(step_name)
    print("=" * 70)

    result = subprocess.run(
        ["python", script_name],
        cwd=BASE_DIR,
        capture_output=True,
        text=True
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"Erreur pendant l'exécution de {script_name}")

    print(f"{script_name} terminé avec succès.\n")


def generate_lineage():
    today = datetime.today().strftime("%Y-%m-%d")

    lineage = {
        "date_execution": today,
        "source": "https://fr.hespress.com/",
        "pipeline_steps": [
            "scraping",
            "cleaning",
            "quality_checks",
            "analysis",
            "data_warehouse",
            "pipeline_metrics",
            "export_to_powerbi"
        ],
        "outputs": {
            "bronze": f"data/lake/bronze/hespress/{today}/articles.json",
            "silver": f"data/lake/silver/hespress/{today}/articles_clean.json",
            "gold": f"data/lake/gold/hespress/{today}/stats.json",
            "warehouse": "data/warehouse/hespress/latest/",
            "pipeline_metrics": "data/warehouse/hespress/latest/pipeline_metrics.csv",
            "streaming_metrics": "data/warehouse/hespress/latest/streaming_metrics.csv",
            "streaming_events": "data/warehouse/hespress/latest/streaming_events.csv",
            "refresh_log": "data/warehouse/hespress/latest/refresh_log.csv"
        }
    }

    governance_dir = BASE_DIR / "data" / "governance"
    governance_dir.mkdir(parents=True, exist_ok=True)

    lineage_file = governance_dir / "lineage.json"

    with open(lineage_file, "w", encoding="utf-8") as f:
        json.dump(lineage, f, indent=2, ensure_ascii=False)

    print("Gouvernance générée : data/governance/lineage.json")


def main():
    print("Démarrage du pipeline Big Data Hespress")

    run_step("Étape 1 : Scraping des articles", "scraper.py")
    run_step("Étape 2 : Nettoyage des données", "clean_data.py")
    run_step("Étape 3 : Contrôles qualité", "quality_checks.py")
    run_step("Étape 4 : Analyse des données", "analyze_data.py")
    run_step("Étape 5 : Data Warehouse", "warehouse.py")
    run_step("Étape 6 : Génération des métriques Bronze / Silver / Gold", "generate_pipeline_metrics.py")
    run_step("Étape 7 : Export vers Power BI latest", "export_to_powerbi.py")

    generate_lineage()

    print("=" * 70)
    print("Pipeline batch terminé avec succès.")
    print("Tu peux maintenant actualiser Power BI.")
    print("=" * 70)


if __name__ == "__main__":
    main()