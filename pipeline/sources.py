from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO, StringIO
from pathlib import Path

import pandas as pd
import requests

from pipeline.config import SECOP_TIMEOUT_SECONDS, SOCRATA_APP_TOKEN, SOCRATA_BASE_URL, SOCRATA_PAGE_SIZE


@dataclass
class FetchResult:
    source_name: str
    dataframe: pd.DataFrame
    raw_path: Path | None
    mode: str


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_raw_bytes(directory: Path, suffix: str, payload: bytes) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / f"{_timestamp_slug()}{suffix}"
    output_path.write_bytes(payload)
    return output_path


def _write_raw_dataframe(directory: Path, dataframe: pd.DataFrame) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    output_path = directory / f"{_timestamp_slug()}.parquet"
    dataframe.to_parquet(output_path, index=False)
    return output_path


def _read_local_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(file_path)
    if suffix in {".json", ".jsonl"}:
        text = file_path.read_text(encoding="utf-8")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            rows = [json.loads(line) for line in text.splitlines() if line.strip()]
            return pd.json_normalize(rows)
        return _json_to_dataframe(payload)
    return pd.read_csv(file_path)


def _json_to_dataframe(payload: object) -> pd.DataFrame:
    if isinstance(payload, list):
        return pd.json_normalize(payload)
    if isinstance(payload, dict):
        list_like_value = next((value for value in payload.values() if isinstance(value, list)), None)
        if list_like_value is not None:
            return pd.json_normalize(list_like_value)
        return pd.json_normalize([payload])
    return pd.DataFrame()


def _flatten_object_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return dataframe

    frame = dataframe.copy()
    object_columns = [
        column
        for column in frame.columns
        if frame[column].apply(lambda value: isinstance(value, dict)).any()
    ]

    for column in object_columns:
        nested = pd.json_normalize(frame[column].dropna())
        nested.index = frame[column].dropna().index
        nested.columns = [f"{column}_{nested_column}" for nested_column in nested.columns]
        frame = frame.drop(columns=[column]).join(nested, how="left")

    return frame


def _read_response(response: requests.Response) -> pd.DataFrame:
    content_type = response.headers.get("content-type", "").lower()
    url_lower = response.url.lower()

    if "json" in content_type or url_lower.endswith(".json"):
        return _json_to_dataframe(response.json())

    if "csv" in content_type or url_lower.endswith(".csv"):
        return pd.read_csv(StringIO(response.text))

    try:
        return pd.read_csv(StringIO(response.text))
    except Exception:
        try:
            return _json_to_dataframe(response.json())
        except Exception:
            return pd.read_parquet(BytesIO(response.content))


def _build_socrata_url(dataset_id: str) -> str:
    return f"{SOCRATA_BASE_URL}/resource/{dataset_id}.json"


def _fetch_socrata_dataset(source_name: str, dataset_id: str, update_field: str, raw_dir: Path, retention_days: int) -> FetchResult:
    headers = {"Accept": "application/json"}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    since = (datetime.now(timezone.utc) - timedelta(days=retention_days)).strftime("%Y-%m-%dT00:00:00.000")
    session = requests.Session()
    rows: list[dict] = []
    offset = 0
    page_size = SOCRATA_PAGE_SIZE

    while True:
        try:
            response = session.get(
                _build_socrata_url(dataset_id),
                params={
                    "$limit": page_size,
                    "$offset": offset,
                    "$order": f"{update_field} ASC",
                    "$where": f"{update_field} >= '{since}'",
                },
                headers=headers,
                timeout=SECOP_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except requests.HTTPError as error:
            status_code = error.response.status_code if error.response is not None else None
            if status_code and status_code >= 500 and page_size > 1000:
                page_size = max(1000, page_size // 2)
                continue
            raise

        page = response.json()
        if not isinstance(page, list) or not page:
            break

        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size

    dataframe = _flatten_object_columns(pd.json_normalize(rows) if rows else pd.DataFrame())
    raw_path = _write_raw_dataframe(raw_dir, dataframe)
    return FetchResult(source_name=source_name, dataframe=dataframe, raw_path=raw_path, mode="socrata")


def fetch_source(
    source_name: str,
    url: str,
    local_file: str,
    raw_dir: Path,
    dataset_id: str,
    update_field: str,
    retention_days: int,
) -> FetchResult:
    if local_file:
        file_path = Path(local_file)
        dataframe = _flatten_object_columns(_read_local_file(file_path))
        return FetchResult(source_name=source_name, dataframe=dataframe, raw_path=file_path, mode="local")

    if dataset_id:
        return _fetch_socrata_dataset(source_name, dataset_id, update_field, raw_dir, retention_days)

    if not url:
        return FetchResult(source_name=source_name, dataframe=pd.DataFrame(), raw_path=None, mode="missing")

    response = requests.get(url, timeout=SECOP_TIMEOUT_SECONDS)
    response.raise_for_status()

    suffix = ".json" if "json" in response.headers.get("content-type", "").lower() else ".csv"
    raw_path = _write_raw_bytes(raw_dir, suffix, response.content)
    dataframe = _flatten_object_columns(_read_response(response))

    return FetchResult(source_name=source_name, dataframe=dataframe, raw_path=raw_path, mode="remote")
