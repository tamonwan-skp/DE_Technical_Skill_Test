from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from google.cloud import storage
import pandas as pd
from datetime import datetime, timedelta
import logging
from io import StringIO

BUCKET_NAME = Variable.get("gcs_bucket_name", default_var="raw-data-de-project1")
BQ_PROJECT = Variable.get("bq_project_id", default_var="my-etl-project-452702")
BQ_DATASET = Variable.get("bq_dataset", default_var="etl_dataset")
BQ_TABLE_PRODUCT = "products"


#  Validate Product Data (Check only if file exists)
def validate_product_data(**kwargs):
    file_name = "products.csv"
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw_data/{file_name}")

    if not blob.exists():
        logging.error(f" ERROR DETECTED: No {file_name} in GCS bucket: {BUCKET_NAME}")
        raise FileNotFoundError(f" No {file_name} in GCS bucket: {BUCKET_NAME}")

    # read data from GCS
    try:
        data = pd.read_csv(StringIO(blob.download_as_text()))
    except Exception as e:
        logging.error(f" ERROR DETECTED: Error reading {file_name} from GCS: {e}")
        raise ValueError(f" Error reading {file_name} from GCS: {e}")

    logging.info(" Product Data File Exists and Read Successfully.")
    return data.to_dict()

#  Transform Product Data 
def transform_product_data(**kwargs):
    ti = kwargs["ti"]
    data_dict = ti.xcom_pull(task_ids="validate_product_data")
    data = pd.DataFrame.from_dict(data_dict)

    # Delete Null 'product_id'
    data = data.dropna(subset=["product_id"])
    data = data[data["product_id"] != ""]

    # Fill missing data
    data["product_name"] = data["product_name"].fillna("Unknown Product")
    data["category"] = data["category"].fillna("Unknown Category")

    # Check duplicate
    if data["product_id"].duplicated().any():
        logging.error(" ERROR DETECTED: Validation Failed: พบค่า product_id ซ้ำกันในไฟล์ข้อมูล.")
        raise ValueError(" Validation Failed: พบค่า product_id ซ้ำกันในไฟล์ข้อมูล.")

    try:
        csv_buffer = StringIO()
        data.to_csv(csv_buffer, index=False)

        gcs_client = storage.Client()
        bucket = gcs_client.bucket(BUCKET_NAME)
        bucket.blob(f"transformed/products/{kwargs['file_name']}").upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

        logging.info(" Product Data Transformation completed.")
    except Exception as e:
        logging.error(f" ERROR DETECTED: Error uploading transformed file to GCS: {e}")
        raise ValueError(f"Error uploading transformed file to GCS: {e}")

    return "Transformation completed."

#  ตั้งค่า DAG สำหรับ Product ETL
with DAG(
    "product_etl",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "execution_timeout": timedelta(minutes=15)  
    }
) as dag:

    validate_product = PythonOperator(
        task_id="validate_product_data",
        python_callable=validate_product_data,
        op_kwargs={"file_name": "product_file.csv"},
    )

    transform_product = PythonOperator(
        task_id="transform_product_data",
        python_callable=transform_product_data,
        op_kwargs={"file_name": "product_file.csv"},
    )

    load_product = GCSToBigQueryOperator(
    task_id="load_product_to_bigquery",
    bucket=BUCKET_NAME,
    source_objects=["transformed/products/product_file.csv"],
    destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_PRODUCT}",
    source_format="CSV",
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    autodetect=False, 
    schema_fields=[
        {"name": "product_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
    ],
    )

    # Workflow DAG
    validate_product >> transform_product >> load_product
