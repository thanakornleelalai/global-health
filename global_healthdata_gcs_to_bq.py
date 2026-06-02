from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

# ==============================
# DEFAULT ARGS
# ==============================

default_args = {
    "owner": "leelalai.t2539@gmail.com",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ==============================
# PROJECT CONFIG
# ==============================

project_id = "apt-summer-479708-b8"

staging_dataset_id = "staging"
transformed_dataset_id = "transformed"
reporting_dataset_id = "reporting"

source_table = f"{project_id}.{staging_dataset_id}.global_health_statistics"

bucket_name = "apt-summer-479708-b8-landing"
file_name = "global_health_statistics.csv"

countries = [
    "argentina", "australia", "brazil", "canada", "china",
    "france", "germany", "india", "indonesia", "italy",
    "japan", "mexico", "nigeria", "russia", "saudi arabia",
    "south africa", "south korea", "turkey", "uk", "usa"
]

QUERY_SQL_PATH = "sql_scripts/"

# ==============================
# DAG
# ==============================

with DAG(
    dag_id="elt_pipeline_global_health",
    default_args=default_args,
    description="ELT Pipeline: GCS → BigQuery → Country Tables & Views",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["gcs", "bigquery", "elt"],
) as dag:

    # 1️⃣ ตรวจสอบว่าไฟล์อยู่ใน GCS
    check_if_file_exists = GCSObjectExistenceSensor(
        task_id="check_if_file_exists",
        bucket=bucket_name,
        object=file_name,
        timeout=300,
        poke_interval=30,
        mode="poke",
    )

    # 2️⃣ โหลด CSV เข้า BigQuery (staging)
    load_csv_to_bigquery = GCSToBigQueryOperator(
        task_id="load_csv_to_bigquery",
        bucket=bucket_name,
        source_objects=[file_name],
        destination_project_dataset_table=source_table,
        source_format="CSV",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        field_delimiter=",",
        autodetect=True,
    )

    create_view_tasks = []

    for country in countries:
        # สร้าง task_id_clean เพื่อใช้เป็น task_id ที่ไม่มีช่องว่าง
        task_id_clean = country.replace(" ", "_")

        create_table_task = BigQueryInsertJobOperator(
            task_id=f"create_table_{task_id_clean}", # ใช้ชื่อที่แก้แล้ว
            configuration={
                "query": {
                    "query": "{% include '" + QUERY_SQL_PATH + "create_table.sql' %}",
                    "useLegacySql": False,
                }
            },
            params={
                "PROJECT_ID": project_id,
                "STAGING_DATASET_ID": staging_dataset_id,
                "TRANSFORMED_DATASET_ID": transformed_dataset_id,
                "COUNTRY": country, # ส่งชื่อประเทศแบบเดิมที่มีช่องว่างไปให้ SQL
            },
            location="US",
        )

        create_view_task = BigQueryInsertJobOperator(
            task_id=f"create_view_{task_id_clean}", # ใช้ชื่อที่แก้แล้ว
            configuration={
                "query": {
                    "query": "{% include '" + QUERY_SQL_PATH + "create_view.sql' %}",
                    "useLegacySql": False,
                }
            },
            params={
                "PROJECT_ID": project_id,
                "TRANSFORMED_DATASET_ID": transformed_dataset_id,
                "REPORTING_DATASET_ID": reporting_dataset_id,
                "COUNTRY": country, # ส่งชื่อประเทศแบบเดิมที่มีช่องว่างไปให้ SQL
            },
            location="US",
        )

        load_csv_to_bigquery >> create_table_task >> create_view_task
        create_view_tasks.append(create_view_task)

    success = EmptyOperator(task_id="success")

    check_if_file_exists >> load_csv_to_bigquery
    # เชื่อมต่อจากสร้าง view ทั้งหมดไปยัง task success
    create_view_tasks >> success