import glob
import json
import logging
import os
from datetime import datetime
import psycopg2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
DB_HOST = os.getenv("WAREHOUSE_DB_HOST", "warehouse-db")
DB_PORT = int(os.getenv("WAREHOUSE_DB_PORT", 5432))
DB_NAME = os.getenv("WAREHOUSE_DB_NAME", "warehouse")
DB_USER = os.getenv("WAREHOUSE_DB_USER", "warehouse")
DB_PASSWORD = os.getenv("WAREHOUSE_DB_PASSWORD", "warehouse")


def get_latest_gold_file() -> str | None:
    files = sorted(glob.glob(os.path.join(DATA_DIR, "gold", "aljazeera_gold_metrics_*.json")))
    return files[-1] if files else None


def connect_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def create_tables(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_summary (
            run_id TEXT PRIMARY KEY,
            total_articles INTEGER,
            generated_at TIMESTAMP,
            imported_at TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_articles_by_source (
            run_id TEXT,
            source TEXT,
            count INTEGER,
            imported_at TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_articles_by_category (
            run_id TEXT,
            category TEXT,
            count INTEGER,
            imported_at TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_articles_by_language (
            run_id TEXT,
            language TEXT,
            count INTEGER,
            imported_at TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_articles_per_day (
            run_id TEXT,
            day DATE,
            count INTEGER,
            imported_at TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_top_keywords (
            run_id TEXT,
            keyword TEXT,
            count INTEGER,
            imported_at TIMESTAMP
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS warehouse_articles_by_country (
            run_id TEXT,
            country TEXT,
            count INTEGER,
            imported_at TIMESTAMP
        )
        """
    )


def insert_metrics(cur, run_id, metrics):
    imported_at = datetime.utcnow()

    # Nettoyer les anciennes données de ce run pour éviter les doublons
    cur.execute("DELETE FROM warehouse_articles_by_source WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM warehouse_articles_by_category WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM warehouse_articles_by_language WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM warehouse_articles_per_day WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM warehouse_top_keywords WHERE run_id = %s", (run_id,))
    cur.execute("DELETE FROM warehouse_articles_by_country WHERE run_id = %s", (run_id,))

    cur.execute(
        "INSERT INTO warehouse_summary (run_id, total_articles, generated_at, imported_at) VALUES (%s, %s, %s, %s) ON CONFLICT (run_id) DO UPDATE SET total_articles = EXCLUDED.total_articles, generated_at = EXCLUDED.generated_at, imported_at = EXCLUDED.imported_at",
        (run_id, metrics.get("total_articles", 0), datetime.utcnow(), imported_at),
    )

    for source, count in metrics.get("articles_by_source", {}).items():
        cur.execute(
            "INSERT INTO warehouse_articles_by_source (run_id, source, count, imported_at) VALUES (%s, %s, %s, %s)",
            (run_id, source, count, imported_at),
        )
    for category, count in metrics.get("articles_by_category", {}).items():
        cur.execute(
            "INSERT INTO warehouse_articles_by_category (run_id, category, count, imported_at) VALUES (%s, %s, %s, %s)",
            (run_id, category, count, imported_at),
        )
    for language, count in metrics.get("articles_by_language", {}).items():
        cur.execute(
            "INSERT INTO warehouse_articles_by_language (run_id, language, count, imported_at) VALUES (%s, %s, %s, %s)",
            (run_id, language, count, imported_at),
        )
    for day, count in metrics.get("articles_per_day", {}).items():
        try:
            day_value = datetime.fromisoformat(day).date()
        except Exception:
            day_value = None
        cur.execute(
            "INSERT INTO warehouse_articles_per_day (run_id, day, count, imported_at) VALUES (%s, %s, %s, %s)",
            (run_id, day_value, count, imported_at),
        )
    for keyword, count in metrics.get("top_keywords", []):
        cur.execute(
            "INSERT INTO warehouse_top_keywords (run_id, keyword, count, imported_at) VALUES (%s, %s, %s, %s)",
            (run_id, keyword, count, imported_at),
        )
    for country, count in metrics.get("articles_by_country", {}).items():
        cur.execute(
            "INSERT INTO warehouse_articles_by_country (run_id, country, count, imported_at) VALUES (%s, %s, %s, %s)",
            (run_id, country, count, imported_at),
        )


def run():
    filepath = get_latest_gold_file()
    if not filepath:
        logging.warning("Aucun fichier gold trouvé. Chargement en entrepôt impossible.")
        return

    logging.info(f"Chargement du fichier gold {filepath} vers l'entrepôt data warehouse.")
    with open(filepath, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    run_id = os.path.basename(filepath).replace(".json", "")

    conn = connect_db()
    try:
        with conn:
            with conn.cursor() as cur:
                create_tables(cur)
                insert_metrics(cur, run_id, metrics)
        logging.info("Import warehouse terminé.")
    finally:
        conn.close()


if __name__ == "__main__":
    run()
