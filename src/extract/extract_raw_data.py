import pandas as pd
from google.cloud import bigquery

def extract_from_csv(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def extract_from_bigquery(query: str, project_id: str) -> pd.DataFrame:
    client = bigquery.Client(project=project_id)
    return client.query(query).to_dataframe()
