import pandas as pd
from google.cloud import bigquery
import yaml
from pathlib import Path

from src.extract.extract_table import extract_table_local, extract_table_gcp
from src.utils.resolve_dates import get_run_month, resolve_etl_dates


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

def run_extraction(config_path: str, output_dir: Path):
    config = load_config(config_path)
    mode = config.get("mode", "gcp")
    run_month = get_run_month()

    # Resolve dynamic calendar dates for filtering
    date_vars = resolve_etl_dates(run_month, None if mode == "local" else bigquery.Client())

    output_dir.mkdir(parents=True, exist_ok=True)

    for dataset_key, dataset_cfg in config["datasets"].items():
        dataset_path = Path(dataset_cfg["path"])
        for table_cfg in dataset_cfg["tables"]:
            name = table_cfg["name"]
            raw_filter = table_cfg.get("filter", "")
            filter_clause = raw_filter.format(**date_vars) if raw_filter else ""

            if mode == "local":
                file_path = dataset_path / name
                print(f"[local] Loading {file_path}")
                df = extract_table_local(file_path, filter_clause)
            else:
                print(f"[gcp] Extracting {dataset_key}.{name}")
                df = extract_table_gcp(bigquery.Client(), dataset_key, name, filter_clause)

            save_path = output_dir / dataset_key / f"{Path(name).stem}.parquet"
            save_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(save_path, index=False)
            print(f"✅ Saved to {save_path}")
