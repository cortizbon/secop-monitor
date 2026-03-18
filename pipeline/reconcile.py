from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd

from pipeline.config import COMMON_COLUMNS



def empty_contracts_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=COMMON_COLUMNS)



def load_contracts(path: Path) -> pd.DataFrame:
    if not path.exists():
        return empty_contracts_frame()

    dataframe = pd.read_parquet(path)
    for column in ["published_at", "updated_at", "contract_date", "ingestion_date", "reference_date"]:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce", utc=True)
    return dataframe



def merge_contracts(existing_df: pd.DataFrame, incoming_df: pd.DataFrame) -> pd.DataFrame:
    if existing_df.empty and incoming_df.empty:
        return empty_contracts_frame()

    combined = pd.concat([existing_df, incoming_df], ignore_index=True)
    if combined.empty:
        return empty_contracts_frame()

    for column in ["updated_at", "published_at", "contract_date", "ingestion_date", "reference_date"]:
        combined[column] = pd.to_datetime(combined[column], errors="coerce", utc=True)

    combined = combined.sort_values(
        by=["reference_date", "updated_at", "ingestion_date"],
        ascending=[False, False, False],
        na_position="last",
    )
    combined = combined.drop_duplicates(subset=["contract_uid"], keep="first")
    return combined.reset_index(drop=True)



def apply_retention(dataframe: pd.DataFrame, now: pd.Timestamp, retention_days: int) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    cutoff = now - timedelta(days=retention_days)
    filtered = dataframe[dataframe["reference_date"].notna() & (dataframe["reference_date"] >= cutoff)].copy()
    return filtered.sort_values("reference_date", ascending=False).reset_index(drop=True)



def build_window(dataframe: pd.DataFrame, now: pd.Timestamp, days: int) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe.copy()

    cutoff = now - timedelta(days=days)
    window = dataframe[dataframe["reference_date"].notna() & (dataframe["reference_date"] >= cutoff)].copy()
    return window.sort_values("reference_date", ascending=False).reset_index(drop=True)



def build_daily_metrics(dataframe: pd.DataFrame, snapshot_date: pd.Timestamp) -> pd.DataFrame:
    amount_series = pd.to_numeric(dataframe.get("amount_value"), errors="coerce") if not dataframe.empty else pd.Series(dtype="float64")
    total_amount = float(amount_series.fillna(0).sum()) if not dataframe.empty else 0.0
    return pd.DataFrame(
        [
            {
                "snapshot_date": snapshot_date,
                "total_contracts": int(len(dataframe)),
                "total_amount": total_amount,
                "unique_entities": int(dataframe["entity_name"].nunique(dropna=True)) if not dataframe.empty else 0,
                "unique_suppliers": int(dataframe["supplier_name"].nunique(dropna=True)) if not dataframe.empty else 0,
            }
        ]
    )



def build_entity_metrics(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=["entity_name", "contracts", "total_amount"])

    metrics = (
        dataframe.dropna(subset=["entity_name"])
        .groupby("entity_name", dropna=False)
        .agg(contracts=("contract_uid", "count"), total_amount=("amount_value", "sum"))
        .reset_index()
        .sort_values(["total_amount", "contracts"], ascending=[False, False])
    )
    return metrics



def build_modality_metrics(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return pd.DataFrame(columns=["modality", "contracts", "total_amount"])

    metrics = (
        dataframe.dropna(subset=["modality"])
        .groupby("modality", dropna=False)
        .agg(contracts=("contract_uid", "count"), total_amount=("amount_value", "sum"))
        .reset_index()
        .sort_values(["total_amount", "contracts"], ascending=[False, False])
    )
    return metrics



def save_parquet(dataframe: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe.to_parquet(path, index=False)
