"""@bruin
name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: day
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
@bruin"""

import os
import json
import pandas as pd
import requests
import io

def materialize():
    vars = json.loads(os.environ["BRUIN_VARS"])
    start_date = vars.get("start_datetime", "2024-01-01")
    end_date = vars.get("end_datetime", "2024-01-31")
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    months = pd.date_range(start=start_date, end=end_date, freq="MS")

    frames = []
    for taxi_type in taxi_types:
        for month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{month.strftime('%Y-%m')}.parquet"
            response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})
            
            if response.status_code != 200:
                print(f"Skipping {url} - status {response.status_code}")
                continue
                
            df = pd.read_parquet(io.BytesIO(response.content))
            df["taxi_type"] = taxi_type
            frames.append(df)

    return pd.concat(frames, ignore_index=True)