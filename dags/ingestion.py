import os
import re
import csv
import sqlite3
import io
from typing import List, Tuple
from datetime import datetime

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.operators.bash import BashOperator

from utils import get_bucket

@dag(
    dag_id='ingestion',
    default_args={
        'owner': 'data-engineering',
    },
    schedule_interval='30 * * * *',
    start_date=days_ago(1),
    catchup=False,
)
def open_meteo_ingestion():
    # 4DkQ3qHHZxTJHeORFYAzouH8KXjv6ameZW9Q8Q/X
    @task
    def find_latest_success_path() -> str:
        """Find the most recent path that contains a .success file"""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        
        # Get S3 connection and bucket
        s3 = S3Hook(aws_conn_id='aws_bucket')
        bucket_name = get_bucket()
        
        objects = s3.list_keys(bucket_name=bucket_name, prefix='')
        print(f"objects {objects}")

        prefix_to_start_dt = {}
        for obj_key in objects:
            if not obj_key or '@' not in obj_key:
                continue
            # Extract the top-level prefix (folder) before the first '/'
            prefix = obj_key.split('/', 1)[0]
            if '@' not in prefix:
                continue
            if prefix in prefix_to_start_dt:
                continue
            try:
                date_part = prefix.split('@', 1)[0].strip()
                print(f"date_part: {date_part}")
                # Support both "YYYY-MM-DDTHH:MM:SS+ZZ:ZZ" and with space separator
                iso_like = date_part.replace('T', ' ')
                date_obj = datetime.fromisoformat(iso_like)
                print(f"date_obj: {date_obj}")
                prefix_to_start_dt[prefix] = date_obj
            except Exception as e:
                print(f"Failed to parse date from prefix '{prefix}': {e}")
                continue
        
        # Sort prefixes by start datetime in descending order
        path_dates = sorted(prefix_to_start_dt.items(), key=lambda x: x[1], reverse=True)
        print(f"path_dates: {path_dates}")
        # Find the first prefix that contains a .success file
        selected_path = None
        for path, _date_obj in [(p, d) for p, d in path_dates]:
            success_key = f"{path.rstrip('/')}/.success"
            print(f"success_key: {success_key}")
            if s3.check_for_key(success_key, bucket_name=bucket_name):
                selected_path = path
                break
        
        if not selected_path:
            raise ValueError("No path with .success file found")
        
        print(f"Selected path: {selected_path}")
        return selected_path

    @task
    def create_staging_table():
        """Create the staging table in PostgreSQL"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS weather_staging (
            time TEXT NOT NULL,
            temperature_2m REAL,
            relativehumidity_2m REAL,
            city TEXT NOT NULL,
            CONSTRAINT weather_staging_time_city_uk UNIQUE (time, city)
        );
        """
        
        postgres_hook.run(create_table_sql)
        return "Staging table created successfully"
    @task
    def upsert_task(latest_prefix: str) -> str:
        """Read CSVs under the latest prefix and upsert rows into staging by (time, city)."""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        s3 = S3Hook(aws_conn_id='aws_bucket')
        bucket_name = get_bucket()
        prefix = f"{latest_prefix.rstrip('/')}/"

        keys = s3.list_keys(bucket_name=bucket_name, prefix=prefix) or []
        csv_keys = [k for k in keys if k.lower().endswith('.csv')]
        print(f"csv_keys: {csv_keys}")

        if not csv_keys:
            return "No CSV files to upsert"

        pg = PostgresHook(postgres_conn_id='postgres_default')
        insert_sql = (
            """
            INSERT INTO weather_staging (time, temperature_2m, relativehumidity_2m, city)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (time, city)
            DO UPDATE SET
                temperature_2m = EXCLUDED.temperature_2m,
                relativehumidity_2m = EXCLUDED.relativehumidity_2m
            """
        )

        total_rows = 0
        with pg.get_conn() as conn:
            with conn.cursor() as cur:
                for key in csv_keys:
                    content = s3.read_key(key=key, bucket_name=bucket_name)
                    f = io.StringIO(content)
                    reader = csv.DictReader(f)
                    batch = []
                    for row in reader:
                        time_val = row.get('time')
                        temp_val = row.get('temperature_2m')
                        rh_val = row.get('relativehumidity_2m')
                        city_val = row.get('city')
                        batch.append(
                            (
                                time_val,
                                float(temp_val) if temp_val not in (None, "") else None,
                                float(rh_val) if rh_val not in (None, "") else None,
                                city_val,
                            )
                        )
                    if batch:
                        cur.executemany(insert_sql, batch)
                        total_rows += len(batch)
            conn.commit()

        return f"Upserted {total_rows} rows from {len(csv_keys)} files"



    # Define tasks
    latest_path = find_latest_success_path()
    staging_table = create_staging_table()
    upsert = upsert_task(latest_path)


    dbt_run = BashOperator(
        task_id="run_dbt",
        bash_command="cd /usr/local/airflow/dbt && dbt run"
    )

    # Dependencies: run dbt after upsert
    latest_path >> staging_table >> upsert >> dbt_run


open_meteo_ingestion()