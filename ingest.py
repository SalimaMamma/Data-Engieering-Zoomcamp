import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

# ---------------- CONFIG ----------------
POSTGRES_URL = "postgresql://postgres:postgres@localhost:5433/ny_taxi"

GREEN_PARQUET = "green_tripdata_2025-11.parquet"
ZONES_CSV = "taxi_zone_lookup.csv"

GREEN_TABLE = "green_trips"
ZONES_TABLE = "zones"
# ----------------------------------------


def ingest_file(file_path, table_name, engine, file_type):
    print(f"\n📥 Ingesting {file_path} → table `{table_name}`")

    if not Path(file_path).exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_type == "parquet":
        df = pd.read_parquet(file_path)
    elif file_type == "csv":
        df = pd.read_csv(file_path)
    else:
        raise ValueError("Unsupported file type")

    print(f"📊 Rows read: {len(df)}")

    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"✅ Data written to table `{table_name}`")


def main():
    print("🔌 Connecting to Postgres...")
    engine = create_engine(POSTGRES_URL)

    with engine.connect():
        print("✅ Connection successful")

    ingest_file(GREEN_PARQUET, GREEN_TABLE, engine, "parquet")
    ingest_file(ZONES_CSV, ZONES_TABLE, engine, "csv")

    print("\n🎉 Ingestion completed successfully")


if __name__ == "__main__":
    main()
