import pandas as pd
from google.cloud import bigquery
from pathlib import Path

def extract_table_local(file_path: Path, filter_clause: str = "") -> pd.DataFrame:
    df = pd.read_csv(file_path) if file_path.suffix == ".csv" else pd.read_parquet(file_path)
    if filter_clause:
        # Use pandas query for simple filters (assumes valid syntax)
        return df.query(filter_clause)
    return df

def extract_table_gcp(client: bigquery.Client, dataset: str, table: str, filter_clause: str) -> pd.DataFrame:
    query = f"SELECT * FROM `{dataset}.{table}` {filter_clause}"
    return client.query(query).to_dataframe()
