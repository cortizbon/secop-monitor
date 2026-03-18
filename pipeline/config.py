from __future__ import annotations

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
CURRENT_DIR = DATA_DIR / "current"
MARTS_DIR = DATA_DIR / "marts"
REPORTS_DIR = BASE_DIR / "reports"

RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "180"))
WEEKLY_DAYS = int(os.getenv("WEEKLY_DAYS", "7"))
MONTHLY_DAYS = int(os.getenv("MONTHLY_DAYS", "30"))
SECOP_TIMEOUT_SECONDS = int(os.getenv("SECOP_TIMEOUT_SECONDS", "120"))
SOCRATA_BASE_URL = os.getenv("SOCRATA_BASE_URL", "https://www.datos.gov.co").strip().rstrip("/")
SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN", "").strip()
SOCRATA_PAGE_SIZE = int(os.getenv("SOCRATA_PAGE_SIZE", "50000"))
SEMANTIC_SEARCH_MODULE = os.getenv("SEMANTIC_SEARCH_MODULE", "").strip()
DEFAULT_SEMANTIC_QUERY = os.getenv("DEFAULT_SEMANTIC_QUERY", "").strip()

SECOP_SOURCES = {
    "secop_i": {
        "url": os.getenv("SECOP_I_URL", "").strip(),
        "local_file": os.getenv("SECOP_I_LOCAL_FILE", "").strip(),
        "dataset_id": os.getenv("SECOP_I_DATASET_ID", "qddk-cgux").strip(),
        "update_field": os.getenv("SECOP_I_UPDATE_FIELD", "ultima_actualizacion").strip(),
        "raw_dir": RAW_DIR / "secop_i",
    },
    "secop_ii": {
        "url": os.getenv("SECOP_II_URL", "").strip(),
        "local_file": os.getenv("SECOP_II_LOCAL_FILE", "").strip(),
        "dataset_id": os.getenv("SECOP_II_DATASET_ID", "ay65-guja").strip(),
        "update_field": os.getenv("SECOP_II_UPDATE_FIELD", "ultima_actualizacion").strip(),
        "raw_dir": RAW_DIR / "secop_ii",
    },
}

CURRENT_CONTRACTS_PATH = CURRENT_DIR / "contracts_last_180_days.parquet"
WEEKLY_CONTRACTS_PATH = CURRENT_DIR / "contracts_last_7_days.parquet"
MONTHLY_CONTRACTS_PATH = CURRENT_DIR / "contracts_last_30_days.parquet"
DAILY_METRICS_PATH = MARTS_DIR / "daily_metrics.parquet"
ENTITY_METRICS_PATH = MARTS_DIR / "entity_metrics.parquet"
MODALITY_METRICS_PATH = MARTS_DIR / "modality_metrics.parquet"

COMMON_COLUMNS = [
    "source_system",
    "contract_uid",
    "process_uid",
    "published_at",
    "updated_at",
    "contract_date",
    "entity_name",
    "supplier_name",
    "amount_value",
    "currency",
    "status",
    "modality",
    "contract_object",
    "region",
    "city",
    "source_url",
    "ingestion_date",
    "record_hash",
    "reference_date",
]

DATE_COLUMNS = [
    "published_at",
    "updated_at",
    "contract_date",
    "ingestion_date",
    "reference_date",
]

TEXT_COLUMNS = [
    "source_system",
    "contract_uid",
    "process_uid",
    "entity_name",
    "supplier_name",
    "currency",
    "status",
    "modality",
    "contract_object",
    "region",
    "city",
    "source_url",
    "record_hash",
]

REPORT_PERIOD_DIRECTORIES = {
    "weekly": REPORTS_DIR / "weekly",
    "monthly": REPORTS_DIR / "monthly",
    "custom": REPORTS_DIR / "custom",
}


def ensure_directories() -> None:
    for directory in [RAW_DIR, CURRENT_DIR, MARTS_DIR, *REPORT_PERIOD_DIRECTORIES.values()]:
        directory.mkdir(parents=True, exist_ok=True)
    for source_config in SECOP_SOURCES.values():
        source_config["raw_dir"].mkdir(parents=True, exist_ok=True)
