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
BQ_TABLE_TRANSACTION = "transactions"
BQ_TABLE_DATE_DIM = "date_dim"


#  ฟังก์ชันแปลงปี พ.ศ. เป็น ค.ศ.
def convert_to_gregorian(year_buddhist):
    return year_buddhist - 543

#  ฟังก์ชันแปลงวันที่จาก พ.ศ. เป็น ค.ศ. และแปลงเป็น datetime
def convert_to_datetime(date_str):
    try:

         # ถ้าเป็น Timestamp อยู่แล้ว ให้ return ค่าเดิม
        if isinstance(date_str, pd.Timestamp):
            return date_str
        
        date_part, time_part = date_str.split(' ') if ' ' in date_str else (date_str, "00:00:00")
        date_parts = date_part.split('-') if '-' in date_part else date_part.split('/')

        if len(date_parts) != 3:
            raise ValueError("Invalid date format")

        if int(date_parts[0]) > 2500:
            year_buddhist = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])
            year_gregorian = convert_to_gregorian(year_buddhist)
        else:
            year_gregorian = int(date_parts[0])
            month = int(date_parts[1])
            day = int(date_parts[2])

        date_time_str = f"{year_gregorian}-{month:02d}-{day:02d} {time_part}"
        return pd.to_datetime(date_time_str, format='%Y-%m-%d %H:%M:%S')

    except Exception as e:
        logging.error(f" Error processing date: {date_str} - {e}")
        return pd.NaT

# Validate Transaction Data
def validate_transaction_data(**kwargs):
    file_name = "transactions.csv"
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"raw_data/{file_name}")

    if not blob.exists():
        logging.error(f" ERROR DETECTED:  No{file_name} in GCS bucket: {BUCKET_NAME}")
        raise FileNotFoundError(f" No {file_name} in GCS bucket: {BUCKET_NAME}")

    try:
        data = pd.read_csv(StringIO(blob.download_as_text()))
    except Exception as e:
        logging.error(f" ERROR DETECTED:  Error reading {file_name} from GCS: {e}")
        raise ValueError(f" Error reading {file_name} from GCS: {e}")

    logging.info(" Transaction Data Validation passed.")
    return data.fillna("").to_dict()

# Transform Transaction Data
def transform_transaction_data(**kwargs):
    ti = kwargs["ti"]
    data_dict = ti.xcom_pull(task_ids="validate_transaction_data")
    data = pd.DataFrame.from_dict(data_dict)

    # Missing and negative values
    data["price"] = data["price"].fillna(0)
    data["price"] = data["price"].apply(lambda x: max(x, 0))
    data["quantity"] = data["quantity"].apply(lambda x: max(x, 0))
    
    # Rename 'date' to 'transaction_date'
    data.rename(columns={"date": "transaction_date"}, inplace=True)
    data["transaction_date"] = data["transaction_date"].apply(convert_to_datetime)

    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)
    bucket.blob("transformed/transactions/transaction_file.csv").upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

    logging.info(" Transaction Data Transformation completed.")
    return "Transformation completed."


# Generate Date Dimension
def create_date_dim(**kwargs):
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(BUCKET_NAME)
    file_name = "transformed/transactions/transaction_file.csv"
    blob = bucket.blob(file_name)

    if not blob.exists():
        raise FileNotFoundError(f" ERROR DETECTED: No {file_name} in GCS")

    data = pd.read_csv(StringIO(blob.download_as_text()))

    if "transaction_date" not in data.columns or data["transaction_date"].isnull().all():
        raise ValueError(" ERROR DETECTED: No transaction_date detail")

    # แปลงวันที่ให้เป็น datetime และแยกเฉพาะวันที่
    data["transaction_date"] = data["transaction_date"].apply(convert_to_datetime)
    unique_dates = data["transaction_date"].dt.date.unique()

    # สร้าง DataFrame และเรียงลำดับตาม `date`
    date_dim_df = pd.DataFrame({"date": unique_dates}).sort_values("date").reset_index(drop=True)

    # เพิ่ม `date_id` ให้เป็น Running Number
    date_dim_df.insert(0, "date_id", range(1, len(date_dim_df) + 1))

    # เปลี่ยน `date` เป็น TIMESTAMP format (เพิ่มเวลา 00:00:00)
    date_dim_df["date"] = date_dim_df["date"].apply(lambda x: f"{x} 00:00:00")

    # เพิ่มคอลัมน์ `day`, `month`, `year`
    date_dim_df["day"] = pd.to_datetime(date_dim_df["date"]).dt.day
    date_dim_df["month"] = pd.to_datetime(date_dim_df["date"]).dt.month
    date_dim_df["year"] = pd.to_datetime(date_dim_df["date"]).dt.year

    # Logging เช็คค่าข้อมูล
    logging.info(f" Date Dimension Created with {len(date_dim_df)} unique dates.")

    # อัปโหลดไฟล์กลับไปยัง GCS
    csv_buffer = StringIO()
    date_dim_df.to_csv(csv_buffer, index=False)
    bucket.blob("transformed/date_dim.csv").upload_from_string(csv_buffer.getvalue(), content_type="text/csv")


# Config 'DAG'
with DAG(
    "transaction_and_date_dim_etl",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    default_args={
        "execution_timeout": timedelta(minutes=15)  
    }
) as dag:

    validate_transaction = PythonOperator(
        task_id="validate_transaction_data",
        python_callable=validate_transaction_data,
        op_kwargs={"file_name": "transactions.csv"},
    )

    transform_transaction = PythonOperator(
        task_id="transform_transaction_data",
        python_callable=transform_transaction_data,
    )

    # Load Transaction to BigQuery
    load_transaction_to_bigquery = GCSToBigQueryOperator(
        task_id="load_transaction_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["transformed/transactions/transaction_file.csv"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_TRANSACTION}",
        source_format="CSV",
        write_disposition="WRITE_APPEND",
        skip_leading_rows=1,
    )

    create_date_dimension = PythonOperator(
        task_id="create_date_dim",
        python_callable=create_date_dim,
    )

    # Load Date Dimension to BigQuery
    load_date_dim_to_bigquery = GCSToBigQueryOperator(
    task_id="load_date_dim_to_bigquery",
    bucket=BUCKET_NAME,
    source_objects=["transformed/date_dim.csv"],
    destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_DATE_DIM}",
    source_format="CSV",
    write_disposition="WRITE_APPEND",
    skip_leading_rows=1,
    autodetect=False,
    schema_fields=[
        {"name": "date_id", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "date", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "day", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "month", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "year", "type": "INTEGER", "mode": "NULLABLE"},
    ]
    )

   
    # Workflow DAG
    validate_transaction >> transform_transaction >> load_transaction_to_bigquery
    load_transaction_to_bigquery >> create_date_dimension >> load_date_dim_to_bigquery
