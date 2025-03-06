from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.cloud import storage
import pandas as pd
from datetime import datetime, timedelta
import logging
from io import StringIO
import re  # ใช้สำหรับเช็ค email format

BUCKET_NAME = Variable.get("gcs_bucket_name", default_var="raw-data-de-project1")
BQ_PROJECT = Variable.get("bq_project_id", default_var="my-etl-project-452702")
BQ_DATASET = Variable.get("bq_dataset", default_var="etl_dataset")
BQ_TABLE_CUSTOMER = "customers"

# Validate Customer Data
def validate_customer_data(**kwargs):
    file_name = "customers.csv"
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw_data/{file_name}")

    if not blob.exists():
        logging.error(f" ERROR DETECTED: No {file_name} in GCS bucket: {BUCKET_NAME}")
        raise FileNotFoundError(f" No {file_name} in GCS bucket: {BUCKET_NAME}")

    try:
        data = pd.read_csv(StringIO(blob.download_as_text()))
    except Exception as e:
        logging.error(f" ERROR DETECTED: Error reading {file_name} from GCS: {e}")
        raise ValueError(f" Error reading {file_name} from GCS: {e}")

    logging.info(" Customer Data File Exists and Read Successfully.")
    return data.to_dict()

#  Transform Customer Data
def transform_customer_data(**kwargs):
    ti = kwargs["ti"]
    data_dict = ti.xcom_pull(task_ids="validate_customer_data")
    data = pd.DataFrame.from_dict(data_dict)

    # Delete `customer_id` is NaN
    data = data.dropna(subset=["customer_id"])

    # 🔹 2. ตรวจสอบ missing values ใน `customer_id`, `name`, `email`
    required_columns = ["customer_id", "name", "email"]
    for col in required_columns:
        if data[col].isnull().any() or (data[col] == "").any():
            logging.error(f" ERROR DETECTED:  {col} is null")
            raise ValueError(f" Validation Failed:  {col} is null")
    
    # 🔹 3. แก้ไขค่า missing
    data["name"] = data["name"].fillna("Unknown")
    data["email"] = data["email"].fillna("unknown@email.com")

    # ลบแถวที่ `customer_id` ซ้ำกัน
    before_drop = len(data)
    data = data.drop_duplicates(subset=["customer_id"], keep="first")
    after_drop = len(data)
    logging.info(f" Removed {before_drop - after_drop} duplicate customer_id rows.")

    # ตรวจสอบ format อีเมล
    def is_valid_email(email):
        return bool(re.match(r"[^@]+@[^@]+\.[^@]+", email))

    invalid_emails = data[~data["email"].apply(is_valid_email)]
    if not invalid_emails.empty:
        logging.error(f" ERROR DETECTED: พบ email ที่ไม่ถูกต้อง:\n{invalid_emails}")
        raise ValueError("Validation Failed: พบ email ที่ไม่ถูกต้อง.")

    # Upload Transformed Data to GCS
    try:
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)

        gcs_client = storage.Client()
        bucket = gcs_client.bucket(BUCKET_NAME)
        bucket.blob(f"transformed/customers/{kwargs['file_name']}").upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

        logging.info(" Customer Data Transformation completed.")
    except Exception as e:
        logging.error(f" ERROR DETECTED: Error uploading transformed file to GCS: {e}")
        raise ValueError(f"Error uploading transformed file to GCS: {e}")

    return "Transformation completed."

# Config DAG for Customer ETL
with DAG(
    "customer_etl",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "execution_timeout": timedelta(minutes=15)  
    }
) as dag:

    validate_customer = PythonOperator(
        task_id="validate_customer_data",
        python_callable=validate_customer_data,
        op_kwargs={"file_name": "customer_file.csv"},
    )

    transform_customer = PythonOperator(
        task_id="transform_customer_data",
        python_callable=transform_customer_data,
        op_kwargs={"file_name": "customer_file.csv"},
    )

    # Load Customer Data to BigQuery 
    load_customer = GCSToBigQueryOperator(
        task_id="load_customer_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["transformed/customers/customer_file.csv"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_CUSTOMER}",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
        schema_fields=[
            {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
        ]
    )

    # Workflow DAG
    validate_customer >> transform_customer >> load_customer
