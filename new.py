from google.cloud import storage, bigquery
from datetime import datetime, timezone
import csv
import os
import random
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO)

# GCP Configurations (Change these!)
PROJECT_ID = os.getenv("PROJECT_ID", "daring-atrium-454004-n4")
BUCKET_NAME = os.getenv("BUCKET_NAME", "assgnment_1")
DATASET_NAME = os.getenv("DATASET_NAME", "assgnment_ds")
TABLE_NAME = os.getenv("TABLE_NAME", "assgnment_t")

def generate_csv():
    """Generates a CSV file with random employee details."""
    
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    filename = f"employee_data_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.csv"

    data = [
        [1, "Alice", random.randint(22, 50), "HR", timestamp],
        [2, "Bob", random.randint(22, 50), "Engineering", timestamp],
        [3, "Charlie", random.randint(22, 50), "Marketing", timestamp]
    ]

    with open(filename, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "name", "age", "department", "timestamp"])
        writer.writerows(data)

    logging.info(f"CSV file created: {filename}")
    return filename

def upload_to_gcs(filename):
    """Uploads the CSV file to Google Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(filename)
    blob.upload_from_filename(filename)

    gcs_uri = f"gs://{BUCKET_NAME}/{filename}"
    logging.info(f"File uploaded to GCS: {gcs_uri}")
    return gcs_uri

def load_to_bigquery(gcs_uri):
    """Loads CSV data from GCS into BigQuery."""
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    load_job.result()

    logging.info(f"Data loaded into BigQuery: {table_id}")
    return f"Data loaded into {table_id}"


def run_pipeline():
    try:
        filename = generate_csv()
        logging.info(f"Checking if file exists: {filename} → {os.path.exists(filename)}")  # ✅ Debug file creation

        gcs_uri = upload_to_gcs(filename)
        bq_status = load_to_bigquery(gcs_uri)
        return f"Pipeline executed successfully: {bq_status}"

    except Exception as e:
        logging.error(f"Error in pipeline: {str(e)}")
        return "Pipeline execution failed"

if __name__ == "__main__":
    print(run_pipeline())
