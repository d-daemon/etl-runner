from pathlib import Path
import pandas as pd

def clean_customer(df: pd.DataFrame) -> pd.DataFrame:
    df["CUST_ID"] = df["CUST_ID"].str.strip().str.upper()
    df["IMAGE_DT"] = pd.to_datetime(df["IMAGE_DT"])
    df = df.drop_duplicates(subset=["CUST_ID", "IMAGE_DT"])
    return df

def stage_table(raw_path: Path, staging_path: Path, table_name: str, clean_func):
    df = pd.read_parquet(raw_path / f"{table_name}.parquet")
    df_clean = clean_func(df)
    staging_path.mkdir(parents=True, exist_ok=True)
    df_clean.to_parquet(staging_path / f"{table_name}.parquet", index=False)
    print(f"✅ Staged {table_name} to {staging_path}")

def run_staging(raw_dir: Path, staging_dir: Path):
    stage_table(raw_dir, staging_dir, "customer", clean_customer)
    # Add other tables similarly: account, transaction, etc.

if __name__ == "__main__":
    raw_dir = Path("output/raw")
    staging_dir = Path("output/staging")
    run_staging(raw_dir, staging_dir)
