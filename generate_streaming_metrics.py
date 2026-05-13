import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
LOCAL_TZ = timezone(timedelta(hours=1))


BASE_DIR = Path(__file__).resolve().parent

TIMEZONE = ZoneInfo("Africa/Casablanca")
NOW = datetime.now(TIMEZONE)
DATE = NOW.strftime("%Y-%m-%d")

# Dossier Gold daté
GOLD_DIR = BASE_DIR / "data" / "lake" / "gold" / "hespress" / DATE

# Dossier stable pour Power BI
POWERBI_DIR = BASE_DIR / "data" / "warehouse" / "hespress" / "latest"

GOLD_STREAMING_METRICS_FILE = GOLD_DIR / "streaming_metrics.csv"
GOLD_STREAMING_EVENTS_FILE = GOLD_DIR / "streaming_events.csv"

POWERBI_STREAMING_METRICS_FILE = POWERBI_DIR / "streaming_metrics.csv"
POWERBI_STREAMING_EVENTS_FILE = POWERBI_DIR / "streaming_events.csv"


def write_streaming_metrics(output_file: Path, row: dict):
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "messages_received",
                "articles_streaming",
                "messages_processed",
                "streaming_errors",
                "status",
                "last_message_time",
            ],
        )
        writer.writeheader()
        writer.writerow(row)

    print(f"Fichier généré : {output_file}")


def write_streaming_events(output_file: Path, rows: list[dict]):
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["timestamp", "messages_count"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Fichier généré : {output_file}")


def main():
    now = datetime.now(LOCAL_TZ)
    last_message_time = now.strftime("%Y-%m-%d %H:%M:%S")

    messages_received = 30
    articles_streaming = 30
    messages_processed = 30
    streaming_errors = 0

    if messages_received == 0:
        status = "Inactif"
    elif streaming_errors > 0:
        status = "Alerte"
    else:
        status = "Actif"

    metrics_row = {
        "messages_received": messages_received,
        "articles_streaming": articles_streaming,
        "messages_processed": messages_processed,
        "streaming_errors": streaming_errors,
        "status": status,
        "last_message_time": last_message_time,
    }

    start_time = now - timedelta(minutes=10)
    values = [3, 5, 4, 6, 7, 5]

    event_rows = []

    for i, value in enumerate(values):
        event_rows.append({
            "timestamp": (start_time + timedelta(minutes=2 * i)).strftime("%Y-%m-%d %H:%M:%S"),
            "messages_count": value,
        })

    write_streaming_metrics(GOLD_STREAMING_METRICS_FILE, metrics_row)
    write_streaming_events(GOLD_STREAMING_EVENTS_FILE, event_rows)

    write_streaming_metrics(POWERBI_STREAMING_METRICS_FILE, metrics_row)
    write_streaming_events(POWERBI_STREAMING_EVENTS_FILE, event_rows)

    print("Métriques streaming générées avec succès.")
    print(metrics_row)


if __name__ == "__main__":
    main()