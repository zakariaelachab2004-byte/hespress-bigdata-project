import csv
from pathlib import Path
from datetime import datetime, timedelta, timezone


BASE_DIR = Path(__file__).resolve().parent

LOCAL_TZ = timezone(timedelta(hours=1))

POWERBI_DIR = BASE_DIR / "data" / "warehouse" / "hespress" / "latest"

STREAMING_METRICS_FILE = POWERBI_DIR / "streaming_metrics.csv"
STREAMING_EVENTS_FILE = POWERBI_DIR / "streaming_events.csv"


def get_current_time() -> str:
    return datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def update_streaming_metrics(
    new_articles_count: int,
    errors_count: int = 0
) -> None:
    """
    Met à jour les métriques Kafka utilisées par Power BI.

    last_check_time = heure de la dernière vérification du producer
    last_message_time = heure du dernier vrai message Kafka
    """

    POWERBI_DIR.mkdir(parents=True, exist_ok=True)

    now = get_current_time()
    last_check_time = now

    if new_articles_count == 0:
        messages_received = 0
        articles_streaming = 0
        messages_processed = 0
        streaming_errors = 0
        status = "En attente"
        last_message_time = "Aucun nouveau message"

    elif errors_count > 0:
        messages_received = new_articles_count
        articles_streaming = new_articles_count
        messages_processed = max(new_articles_count - errors_count, 0)
        streaming_errors = errors_count
        status = "Alerte"
        last_message_time = now

    else:
        messages_received = new_articles_count
        articles_streaming = new_articles_count
        messages_processed = new_articles_count
        streaming_errors = 0
        status = "Actif"
        last_message_time = now

    with open(STREAMING_METRICS_FILE, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "messages_received",
                "articles_streaming",
                "messages_processed",
                "streaming_errors",
                "status",
                "last_message_time",
                "last_check_time",
            ],
        )
        writer.writeheader()
        writer.writerow({
            "messages_received": messages_received,
            "articles_streaming": articles_streaming,
            "messages_processed": messages_processed,
            "streaming_errors": streaming_errors,
            "status": status,
            "last_message_time": last_message_time,
            "last_check_time": last_check_time,
        })

    append_streaming_event(messages_received)

    print("Métriques Kafka mises à jour :")
    print({
        "messages_received": messages_received,
        "articles_streaming": articles_streaming,
        "messages_processed": messages_processed,
        "streaming_errors": streaming_errors,
        "status": status,
        "last_message_time": last_message_time,
        "last_check_time": last_check_time,
    })


def append_streaming_event(messages_count: int) -> None:
    """
    Ajoute un point temporel pour le graphique Power BI :
    Messages Kafka reçus dans le temps.
    """

    POWERBI_DIR.mkdir(parents=True, exist_ok=True)

    now = get_current_time()

    file_exists = STREAMING_EVENTS_FILE.exists()

    with open(STREAMING_EVENTS_FILE, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["timestamp", "messages_count"]
        )

        if not file_exists:
            writer.writeheader()

        writer.writerow({
            "timestamp": now,
            "messages_count": messages_count,
        })