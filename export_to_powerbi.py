import shutil
import csv
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo


BASE_DIR = Path(__file__).resolve().parent

# Date du jour, par exemple : 2026-05-10
DATE = datetime.now().strftime("%Y-%m-%d")

# Dossier Gold daté
GOLD_DIR = BASE_DIR / "data" / "lake" / "gold" / "hespress" / DATE

# Dossier fixe utilisé par Power BI
POWERBI_DIR = BASE_DIR / "data" / "warehouse" / "hespress" / "latest"

FILES_TO_COPY = [
    "articles_by_country.csv",
    "articles_by_theme.csv",
    "articles_per_day.csv",
    "pipeline_metrics.csv",
    "streaming_metrics.csv",
    "streaming_events.csv",
]


def copy_files_to_powerbi():
    POWERBI_DIR.mkdir(parents=True, exist_ok=True)

    for file_name in FILES_TO_COPY:
        source = GOLD_DIR / file_name
        destination = POWERBI_DIR / file_name

        if source.exists():
            shutil.copy2(source, destination)
            print(f"Copié : {source} -> {destination}")
        else:
            print(f"Fichier introuvable : {source}")


def generate_refresh_log():
    refresh_file = POWERBI_DIR / "refresh_log.csv"

    now = datetime.now(ZoneInfo("Africa/Casablanca")).strftime("%Y-%m-%d %H:%M:%S")

    with open(refresh_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["refresh_datetime"])
        writer.writeheader()
        writer.writerow({
            "refresh_datetime": now
        })

    print(f"Fichier refresh généré : {refresh_file}")


def main():
    copy_files_to_powerbi()
    generate_refresh_log()
    print("Export Power BI terminé.")


if __name__ == "__main__":
    main()