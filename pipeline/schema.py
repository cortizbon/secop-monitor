from __future__ import annotations

import hashlib
from typing import Iterable

import pandas as pd

from pipeline.config import COMMON_COLUMNS

COLUMN_ALIASES = {
    "contract_uid": [
        "contract_uid",
        "id_contrato",
        "id_del_contrato",
        "id del contrato",
        "idcontrato",
        "codigo_contrato",
        "nro_contrato",
        "numero_contrato",
        "numero_del_contrato",
        "contrato_id",
        "uid",
        "id",
    ],
    "process_uid": [
        "process_uid",
        "id_proceso",
        "id_del_proceso",
        "id del proceso",
        "proceso_de_compra",
        "proceso_compra",
        "codigo_proceso",
        "nro_proceso",
        "numero_proceso",
        "numero_del_proceso",
        "proceso_id",
    ],
    "published_at": [
        "published_at",
        "fecha_publicacion",
        "fecha de publicacion",
        "fecha_publicado",
        "published_date",
        "date_published",
        "fecha_creacion",
        "fecha_de_cargue_en_el_secop",
        "fecha_de_publicacion_del",
        "fecha_de_publicacion_fase_3",
    ],
    "updated_at": [
        "updated_at",
        "fecha_actualizacion",
        "fecha de actualizacion",
        "last_updated",
        "updated_date",
        "fecha_modificacion",
        "ultima_actualizacion",
        "fecha_de_ultima_publicaci",
    ],
    "contract_date": [
        "contract_date",
        "fecha_firma",
        "fecha_inicio",
        "fecha del contrato",
        "contract_date",
        "signing_date",
        "fecha_de_firma",
        "fecha_de_firma_del_contrato",
    ],
    "entity_name": [
        "entity_name",
        "entidad",
        "nombre_entidad",
        "nombre_de_la_entidad",
        "nombre de la entidad",
        "entidad_nombre",
        "buyer_name",
        "procuring_entity",
    ],
    "supplier_name": [
        "supplier_name",
        "proveedor",
        "contratista",
        "nombre_proveedor",
        "nombre del proveedor",
        "supplier",
        "supplier_name",
        "awarded_supplier",
        "proveedor_adjudicado",
        "nom_razon_social_contratista",
        "nom_raz_social_contratista",
        "nombre_del_proveedor",
    ],
    "amount_value": [
        "amount_value",
        "valor_contrato",
        "valor del contrato",
        "monto",
        "valor",
        "contract_value",
        "amount",
        "valor_total",
        "valor_del_contrato",
        "cuantia_contrato",
        "valor_contrato_con_adiciones",
        "valor_total_adjudicacion",
    ],
    "currency": [
        "currency",
        "moneda",
        "currency_code",
    ],
    "status": [
        "status",
        "estado",
        "estado_contrato",
        "contract_status",
        "estado_del_proceso",
    ],
    "modality": [
        "modality",
        "modalidad",
        "modalidad_contratacion",
        "procurement_method",
        "tipo_proceso",
        "modalidad_de_contratacion",
        "modalidad_de_contrataci_n",
    ],
    "contract_object": [
        "contract_object",
        "objeto",
        "objeto_contrato",
        "descripcion",
        "detalle_objeto",
        "contract_title",
        "title",
        "description",
        "objeto_del_contrato",
        "objeto_del_contrato_a_la",
        "objeto_del_proceso",
        "descripci_n_del_procedimiento",
    ],
    "region": [
        "region",
        "departamento",
        "region_name",
        "departamento_entidad",
        "departamento_proveedor",
    ],
    "city": [
        "city",
        "ciudad",
        "municipio",
        "city_name",
        "municipio_entidad",
        "ciudad_entidad",
    ],
    "source_url": [
        "source_url",
        "url",
        "link",
        "detalle_url",
        "contract_url",
        "urlproceso_url",
        "ruta_proceso_en_secop_i_url",
        "url_contrato",
    ],
}



def _normalize_column_names(columns: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for column in columns:
        normalized = (
            str(column)
            .strip()
            .lower()
            .replace("á", "a")
            .replace("é", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ú", "u")
        )
        normalized = normalized.replace("/", "_").replace("-", "_").replace(" ", "_").replace(".", "_")
        mapping[column] = normalized
    return mapping



def _find_column(df: pd.DataFrame, aliases: list[str]) -> str | None:
    normalized_lookup = {normalized: original for original, normalized in _normalize_column_names(df.columns).items()}
    for alias in aliases:
        normalized_alias = (
            alias.strip().lower().replace(" ", "_").replace("-", "_").replace("/", "_").replace(".", "_")
        )
        if normalized_alias in normalized_lookup:
            return normalized_lookup[normalized_alias]
    return None



def _coalesce_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().replace({"": pd.NA})



def _build_contract_uid(frame: pd.DataFrame) -> pd.Series:
    base = (
        frame["source_system"].fillna("")
        + "|"
        + frame["process_uid"].fillna("")
        + "|"
        + frame["entity_name"].fillna("")
        + "|"
        + frame["supplier_name"].fillna("")
        + "|"
        + frame["contract_object"].fillna("")
    )
    return base.apply(lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()[:24])



def _build_record_hash(frame: pd.DataFrame) -> pd.Series:
    hash_input = frame[
        [
            "contract_uid",
            "process_uid",
            "entity_name",
            "supplier_name",
            "amount_value",
            "status",
            "modality",
            "contract_object",
            "reference_date",
        ]
    ].fillna("")
    return hash_input.astype(str).agg("|".join, axis=1).apply(
        lambda value: hashlib.sha256(value.encode("utf-8")).hexdigest()
    )



def normalize_contracts(raw_df: pd.DataFrame, source_name: str, ingestion_date: pd.Timestamp) -> pd.DataFrame:
    if raw_df.empty:
        return pd.DataFrame(columns=COMMON_COLUMNS)

    frame = raw_df.copy()
    normalized = pd.DataFrame(index=frame.index)
    normalized["source_system"] = source_name

    for canonical_column, aliases in COLUMN_ALIASES.items():
        source_column = _find_column(frame, aliases)
        if source_column is None:
            normalized[canonical_column] = pd.NA
        else:
            normalized[canonical_column] = frame[source_column]

    for column in [
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
    ]:
        normalized[column] = _coalesce_text(normalized[column])

    normalized["amount_value"] = pd.to_numeric(normalized["amount_value"], errors="coerce")

    for date_column in ["published_at", "updated_at", "contract_date"]:
        normalized[date_column] = pd.to_datetime(normalized[date_column], errors="coerce", utc=True)

    normalized["ingestion_date"] = pd.to_datetime(ingestion_date, utc=True)
    normalized["reference_date"] = normalized[["updated_at", "published_at", "contract_date"]].bfill(axis=1).iloc[:, 0]

    missing_contract_uid = normalized["contract_uid"].isna() | (normalized["contract_uid"].astype(str).str.strip() == "")
    normalized.loc[missing_contract_uid, "contract_uid"] = _build_contract_uid(normalized.loc[missing_contract_uid])

    normalized["record_hash"] = _build_record_hash(normalized)

    normalized = normalized[COMMON_COLUMNS].drop_duplicates(subset=["record_hash"]).reset_index(drop=True)
    return normalized
