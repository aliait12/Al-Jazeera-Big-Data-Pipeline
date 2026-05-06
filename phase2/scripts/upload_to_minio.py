import os
import logging
from pathlib import Path
from botocore.exceptions import ClientError
import boto3
from boto3.session import Session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "datalake")

LAYER_PATHS = {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
}


def get_s3_client():
    session = Session(
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )
    return session.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        config=boto3.session.Config(signature_version="s3v4"),
    )


def ensure_bucket(client):
    try:
        client.head_bucket(Bucket=MINIO_BUCKET)
        logging.info(f"Bucket {MINIO_BUCKET} existe déjà.")
    except ClientError:
        logging.info(f"Création du bucket {MINIO_BUCKET}...")
        client.create_bucket(Bucket=MINIO_BUCKET)


def upload_file(client, local_path: Path, object_key: str):
    logging.info(f"Upload {local_path} -> s3://{MINIO_BUCKET}/{object_key}")
    client.upload_file(str(local_path), MINIO_BUCKET, object_key)


def run():
    client = get_s3_client()
    ensure_bucket(client)

    for layer, subdir in LAYER_PATHS.items():
        folder = Path(DATA_DIR) / subdir
        if not folder.exists():
            logging.warning(f"Répertoire absent : {folder}")
            continue

        for path in sorted(folder.glob("**/*.json")):
            key = f"{layer}/{path.name}"
            upload_file(client, path, key)

    logging.info("Upload des fichiers vers MinIO terminé.")


if __name__ == "__main__":
    run()
