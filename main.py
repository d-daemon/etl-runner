from pathlib import Path
from src.extract.extract_t2 import run_extraction

if __name__ == "__main__":
    run_extraction("etl/config/t2_sources.yaml", Path("data/staging/T2"))
