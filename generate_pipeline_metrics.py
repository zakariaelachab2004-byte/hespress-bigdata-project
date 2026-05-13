import json
import csv
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent

DATE = "2026-05-10"

BRONZE_DIR = BASE_DIR / "data" / "lake" / "bronze" / "hespress" / DATE
SILVER_FILE = BASE_DIR / "data" / "lake" / "silver" / "hespress" / DATE / "articles_clean.json"
GOLD_STATS_FILE = BASE_DIR / "data" / "lake" / "gold" / "hespress" / DATE / "stats.json"
OUTPUT_FILE = BASE_DIR / "data" / "lake" / "gold" / "hespress" / DATE / "pipeline_metrics.csv"


def count_json_articles(file_path: Path) -> int:
    if not file_path.exists():
        print(f"Fichier introuvable : {file_path}")
        return 0

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list):
            return len(data)

        if isinstance(data, dict) and "articles" in data:
            return len(data["articles"])

        return 0

    except Exception as e:
        print(f"Erreur lecture {file_path}: {e}")
        return 0


def count_bronze_articles(bronze_dir: Path) -> int:
    if not bronze_dir.exists():
        print(f"Dossier Bronze introuvable : {bronze_dir}")
        return 0

    total = 0

    for json_file in bronze_dir.glob("*.json"):
        total += count_json_articles(json_file)

    return total


def get_gold_articles_count(stats_file: Path) -> int:
    if not stats_file.exists():
        print(f"Fichier Gold stats introuvable : {stats_file}")
        return 0

    try:
        with open(stats_file, "r", encoding="utf-8") as f:
            stats = json.load(f)

        return int(stats.get("total_articles", 0))

    except Exception as e:
        print(f"Erreur lecture {stats_file}: {e}")
        return 0


def main():
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    bronze_count = count_bronze_articles(BRONZE_DIR)
    silver_count = count_json_articles(SILVER_FILE)
    gold_count = get_gold_articles_count(GOLD_STATS_FILE)

    rows = [
        {"layer": "Bronze", "nombre_articles": bronze_count, "ordre": 1},
        {"layer": "Silver", "nombre_articles": silver_count, "ordre": 2},
        {"layer": "Gold", "nombre_articles": gold_count, "ordre": 3},
    ]

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["layer", "nombre_articles", "ordre"])
        writer.writeheader()
        writer.writerows(rows)

    print("pipeline_metrics.csv généré avec succès")
    print(f"Chemin : {OUTPUT_FILE}")
    print(rows)


if __name__ == "__main__":
    main()