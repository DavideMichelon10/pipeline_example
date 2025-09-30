import os
import json
import csv
from datetime import datetime, timezone

import requests
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python import get_current_context

from utils import get_bucket_and_prefix

@dag(
    dag_id='extraction',
    default_args={
        'owner': 'data-engineering',
    },
    schedule_interval='15 */3 * * *',
    start_date=days_ago(1),
    catchup=False
)
def open_meteo_extraction():

    @task
    def get_lat_long() -> list[dict]:
        italian_cities = [
            {'city': 'Torino', 'latitude': 45.07, 'longitude': 7.69},
            {'city': 'Milano', 'latitude': 45.47, 'longitude': 9.19},
            {'city': 'Napoli', 'latitude': 40.85, 'longitude': 14.25},
            {'city': 'Roma', 'latitude': 41.90, 'longitude': 12.49},
            {'city': 'Venezia', 'latitude': 45.44, 'longitude': 12.33},
        ]
        return italian_cities

    @task
    def fetch_raw(**kwargs):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        city = kwargs['city']
        latitude = kwargs['latitude']
        longitude = kwargs['longitude']
        
        start_dt = kwargs['data_interval_start']
        end_dt = kwargs['data_interval_end']
        start_date = start_dt.date().isoformat()
        end_date = end_dt.date().isoformat()
        print(f"start_date: {start_date} e {end_date}")
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'hourly': 'temperature_2m,relativehumidity_2m,weathercode',
            'start_date': start_date,
            'end_date': end_date,
            'timezone': 'GMT',
        }

        url = 'https://api.open-meteo.com/v1/forecast'
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        fetched_at = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        raw_path = os.path.join(f'open_meteo_raw_{fetched_at}.json')
        with open(raw_path, 'w') as f:
            json.dump(data, f)
        
        csv_path = os.path.join(f"{city}.csv")
        hourly = data.get('hourly', {})
        times = hourly.get('time', [])
        temps = hourly.get('temperature_2m', [])
        hums = hourly.get('relativehumidity_2m', [])
        codes = hourly.get('weathercode', [])

        fieldnames = ['city', 'time', 'temperature_2m', 'relativehumidity_2m', 'weathercode']
        with open(csv_path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for idx in range(min(len(times), len(temps), len(hums), len(codes))):
                writer.writerow({
                    'city': city,
                    'time': times[idx],
                    'temperature_2m': temps[idx],
                    'relativehumidity_2m': hums[idx],
                    'weathercode': codes[idx],
                })

        s3_key = None
        bucket, prefix = get_bucket_and_prefix(start_dt, end_dt)
        def _key(path: str) -> str:
            base = os.path.basename(path)
            if prefix:
                return f"{prefix.rstrip('/')}/{base}"
            return base

        s3 = S3Hook(aws_conn_id='aws_bucket')
        s3_key = _key(csv_path)
        s3.load_file(filename=csv_path, key=s3_key, bucket_name=bucket, replace=True)


    @task
    def upload_success() -> dict:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        ctx = get_current_context()
        start_dt = ctx['data_interval_start']
        end_dt = ctx['data_interval_end']
        bucket, prefix = get_bucket_and_prefix(start_dt, end_dt)
        key = f"{prefix.rstrip('/')}/.success"

        s3 = S3Hook(aws_conn_id='aws_bucket')
        s3.load_bytes(b"", key=key, bucket_name=bucket, replace=True)

    
    lat_long = get_lat_long()
    fetch_raw.expand_kwargs(lat_long) >> upload_success()


open_meteo_extraction = open_meteo_extraction()