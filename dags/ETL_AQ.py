from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import json

API_KEY = "917fed820e507d7aed1eb0bc763ac2cc1f8d1f4c9451f294a54e8fb2608f9cac"
LOCATION_ID = 2851582

def fetch_data():
    headers = {"X-API-Key": API_KEY}

    # 1️⃣ Fetch metadata
    meta = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}", headers=headers).json()
    station = meta['results'][0]

    print("\n=== STATION METADATA ===")
    print(f"Station name: {station['name']}")
    print(f"Provider: {station['provider']['name']}")
    print(f"Coordinates: {station['coordinates']}")
    print("Sensors:")
    sensor_map = {}
    for s in station['sensors']:
        sid = s['id']
        display = s['parameter']['displayName']
        units = s['parameter']['units']
        sensor_map[sid] = f"{display} ({units})"
        print(f"  - {display} ({units})")

    # 2️⃣ Fetch latest readings
    data = requests.get(f"https://api.openaq.org/v3/locations/{LOCATION_ID}/latest", headers=headers).json()
    results = data.get('results', [])

    print("\n=== LATEST READINGS ===")
    for r in results:
        sensor_id = r.get('sensorsId') or r.get('sensorId')
        value = r.get('value')
        print(f"{sensor_map.get(sensor_id, sensor_id)}: {value}")

default_args = {'owner': 'moi'}

with DAG('fetch_kech_stations_dag', 
        start_date=datetime(2026, 2, 16),
        schedule_interval=None, catchup=False) as dag:
    
    PythonOperator(task_id='fetch_data', python_callable=fetch_data)
