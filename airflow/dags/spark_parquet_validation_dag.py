# airflow/dags/spark_parquet_validation_dag.py

import os
import pendulum
from pathlib import Path

from airflow.decorators import dag, task

# host: ./data/output/parquet
# airflow: /opt/airflow/data/parquet
DATA_BASE_PATH = Path("/opt/airflow/data/parquet")


@dag(
    dag_id="spark_parquet_validation_dag",
    description="Read Spark parquet output (written by PySpark) and log simple aggregations",
    schedule="*/5 * * * *",  # every 5 minutes
    start_date=pendulum.datetime(2025, 11, 1, tz="Asia/Kolkata"),
    catchup=False,
    tags=["spark", "kafka", "validation"],
)
def spark_parquet_validation_dag():
    @task()
    def aggregate_parquet():
        import pandas as pd

        if not DATA_BASE_PATH.exists():
            print(f"[aggregate_parquet] Path does not exist yet: {DATA_BASE_PATH}")
            print("Spark streaming might not have written any data yet.")
            return

        # Check if there is at least one file
        has_files = any(
            p.is_file() for p in DATA_BASE_PATH.rglob("*.parquet")
        )
        if not has_files:
            print(f"[aggregate_parquet] No .parquet files found under: {DATA_BASE_PATH}")
            return

        print(f"[aggregate_parquet] Reading Parquet from: {DATA_BASE_PATH}")

        # pandas + pyarrow can read a partitioned directory tree
        df = pd.read_parquet(DATA_BASE_PATH, engine="pyarrow")

        print("=== DataFrame head ===")
        print(df.head(10))

        if "id" not in df.columns or "value" not in df.columns:
            print("[aggregate_parquet] Expected columns 'id' and 'value' not found in dataframe.")
            print(f"Columns present: {list(df.columns)}")
            return

        print("=== Count per id ===")
        counts = df.groupby("id")["value"].agg(["count", "mean"]).reset_index()
        print(counts)

        print(f"[aggregate_parquet] Total rows: {len(df)}")

    aggregate_parquet()


dag = spark_parquet_validation_dag()
