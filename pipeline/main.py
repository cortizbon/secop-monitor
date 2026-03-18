from __future__ import annotations

import argparse
from datetime import datetime, timezone

import pandas as pd

from pipeline.config import (
    CURRENT_CONTRACTS_PATH,
    DAILY_METRICS_PATH,
    DEFAULT_SEMANTIC_QUERY,
    ENTITY_METRICS_PATH,
    MODALITY_METRICS_PATH,
    MONTHLY_CONTRACTS_PATH,
    MONTHLY_DAYS,
    REPORT_PERIOD_DIRECTORIES,
    RETENTION_DAYS,
    SECOP_SOURCES,
    WEEKLY_CONTRACTS_PATH,
    WEEKLY_DAYS,
    ensure_directories,
)
from pipeline.reconcile import (
    apply_retention,
    build_daily_metrics,
    build_entity_metrics,
    build_modality_metrics,
    build_window,
    load_contracts,
    merge_contracts,
    save_parquet,
)
from pipeline.reporting import render_report, semantic_report_path
from pipeline.schema import normalize_contracts
from pipeline.semantic import search_contracts
from pipeline.sources import fetch_source



def _now_utc() -> pd.Timestamp:
    return pd.Timestamp.now(tz="UTC")



def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")



def run_daily_pipeline() -> None:
    ensure_directories()
    now = _now_utc()

    normalized_frames: list[pd.DataFrame] = []
    for source_name, source_config in SECOP_SOURCES.items():
        result = fetch_source(
            source_name=source_name,
            url=source_config["url"],
            local_file=source_config["local_file"],
            raw_dir=source_config["raw_dir"],
            dataset_id=source_config["dataset_id"],
            update_field=source_config["update_field"],
            retention_days=RETENTION_DAYS,
        )
        normalized_frames.append(normalize_contracts(result.dataframe, source_name, now))
        print(f"[{source_name}] mode={result.mode} rows={len(result.dataframe)}")

    incoming_contracts = pd.concat(normalized_frames, ignore_index=True) if normalized_frames else pd.DataFrame()
    existing_contracts = load_contracts(CURRENT_CONTRACTS_PATH)

    merged_contracts = merge_contracts(existing_contracts, incoming_contracts)
    current_contracts = apply_retention(merged_contracts, now, RETENTION_DAYS)
    weekly_contracts = build_window(current_contracts, now, WEEKLY_DAYS)
    monthly_contracts = build_window(current_contracts, now, MONTHLY_DAYS)

    save_parquet(current_contracts, CURRENT_CONTRACTS_PATH)
    save_parquet(weekly_contracts, WEEKLY_CONTRACTS_PATH)
    save_parquet(monthly_contracts, MONTHLY_CONTRACTS_PATH)
    save_parquet(build_daily_metrics(current_contracts, now), DAILY_METRICS_PATH)
    save_parquet(build_entity_metrics(current_contracts), ENTITY_METRICS_PATH)
    save_parquet(build_modality_metrics(current_contracts), MODALITY_METRICS_PATH)

    generated_at = now.strftime("%Y-%m-%d %H:%M UTC")
    render_report(
        weekly_contracts,
        REPORT_PERIOD_DIRECTORIES["weekly"] / "weekly_latest.html",
        title="SECOP Monitor · Reporte semanal",
        subtitle=f"Últimos {WEEKLY_DAYS} días",
        generated_at=generated_at,
    )
    render_report(
        monthly_contracts,
        REPORT_PERIOD_DIRECTORIES["monthly"] / "monthly_latest.html",
        title="SECOP Monitor · Reporte mensual",
        subtitle=f"Últimos {MONTHLY_DAYS} días",
        generated_at=generated_at,
    )

    print(
        "Pipeline completado "
        f"current={len(current_contracts)} weekly={len(weekly_contracts)} monthly={len(monthly_contracts)}"
    )



def run_semantic_report(query: str) -> None:
    ensure_directories()
    now = _now_utc()
    weekly_contracts = load_contracts(WEEKLY_CONTRACTS_PATH)
    result = search_contracts(weekly_contracts, query=query, top_k=50)
    output_path = semantic_report_path(REPORT_PERIOD_DIRECTORIES["custom"], query, _timestamp_slug())
    render_report(
        result,
        output_path,
        title="SECOP Monitor · Reporte temático",
        subtitle="Contratos relevantes de la última semana",
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        query=query,
    )
    print(f"Reporte semántico generado en {output_path}")



def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SECOP Monitor MVP")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("daily", help="Ejecuta ingesta, reconciliación y reportes base")

    semantic_parser = subparsers.add_parser("semantic-report", help="Genera un reporte temático")
    semantic_parser.add_argument("--query", default=DEFAULT_SEMANTIC_QUERY, help="Consulta para el reporte")

    return parser



def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "daily":
        run_daily_pipeline()
        return

    if args.command == "semantic-report":
        if not args.query:
            parser.error("La consulta semántica no puede estar vacía.")
        run_semantic_report(args.query)
        return


if __name__ == "__main__":
    main()
