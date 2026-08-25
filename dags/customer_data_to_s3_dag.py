"""
Generates synthetic customer records with Faker (same logic as faker.ipynb)
and uploads the CSV straight to S3 every minute — replaces the NiFi
ListFile -> FetchFile -> PutS3Object flow.

Runs on the EC2 instance provisioned by terraform/main.tf, which already
carries an instance profile (ec2-scd-snowflake-us-west-2-tf-role,
AmazonS3FullAccess), so boto3 picks up credentials automatically — no keys
needed here.

Dependencies (faker, boto3 — see requirements.txt in this folder) are not
part of the base Airflow image and must be installed into the Airflow
containers/venv:

    docker-compose setup (docker-compose.yaml in this repo):
        add to .env next to it:
            PIP_ADDITIONAL_REQUIREMENTS=faker boto3
        then:
            docker-compose down && docker-compose up -d

    plain venv setup:
        pip install -r requirements.txt
"""

import csv
import os
from datetime import datetime, timedelta, timezone

import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker

S3_BUCKET = os.environ.get("SCD_S3_BUCKET", "snowflake-class-project-ingestion")
S3_PREFIX = os.environ.get("SCD_S3_PREFIX", "")
LOCAL_DATA_DIR = os.environ.get("SCD_LOCAL_DATA_DIR", "/tmp/FakeDataset")
RECORD_COUNT = int(os.environ.get("SCD_RECORD_COUNT", "10000"))

FIELDNAMES = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "street",
    "city",
    "state",
    "country",
]


def generate_and_upload_customers(**context):
    """Generate a CSV of fake customer records and upload it to S3."""
    fake = Faker()
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)
    file_path = os.path.join(LOCAL_DATA_DIR, f"customer{timestamp}.csv")

    with open(file_path, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=FIELDNAMES)
        writer.writeheader()

        for customer_id in range(RECORD_COUNT):
            writer.writerow(
                {
                    "customer_id": customer_id,
                    "first_name": fake.first_name(),
                    "last_name": fake.last_name(),
                    "email": fake.email(),
                    "street": fake.street_address(),
                    "city": fake.city(),
                    "state": fake.state(),
                    "country": fake.country(),
                }
            )

    s3_key = (
        f"{S3_PREFIX}{os.path.basename(file_path)}"
        if S3_PREFIX
        else os.path.basename(file_path)
    )
    boto3.client("s3").upload_file(file_path, S3_BUCKET, s3_key)
    os.remove(file_path)


default_args = {
    "owner": "scd-demo",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="customer_data_to_s3",
    description="Generate fake customer CSVs and upload them to S3 (replaces the NiFi flow)",
    default_args=default_args,
    schedule=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["scd-demo", "s3", "faker"],
) as dag:
    generate_and_upload = PythonOperator(
        task_id="generate_and_upload_customers",
        python_callable=generate_and_upload_customers,
    )
