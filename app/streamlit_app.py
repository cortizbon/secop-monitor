from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

BASE_DIR = Path(__file__).resolve().parents[1]
CURRENT_CONTRACTS_PATH = BASE_DIR / "data" / "current" / "contracts_last_180_days.parquet"
DAILY_METRICS_PATH = BASE_DIR / "data" / "marts" / "daily_metrics.parquet"
ENTITY_METRICS_PATH = BASE_DIR / "data" / "marts" / "entity_metrics.parquet"
MODALITY_METRICS_PATH = BASE_DIR / "data" / "marts" / "modality_metrics.parquet"
REPORTS_DIR = BASE_DIR / "reports"

st.set_page_config(page_title="SECOP Monitor", layout="wide")


@st.cache_data(show_spinner=False)
def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    dataframe = pd.read_parquet(path)
    for column in ["published_at", "updated_at", "contract_date", "ingestion_date", "reference_date"]:
        if column in dataframe.columns:
            dataframe[column] = pd.to_datetime(dataframe[column], errors="coerce", utc=True)
    return dataframe



def format_currency(value: float) -> str:
    return f"${value:,.0f}".replace(",", ".")



def list_reports() -> list[Path]:
    return sorted(REPORTS_DIR.glob("**/*.html"), reverse=True)


contracts = load_parquet(CURRENT_CONTRACTS_PATH)
daily_metrics = load_parquet(DAILY_METRICS_PATH)
entity_metrics = load_parquet(ENTITY_METRICS_PATH)
modality_metrics = load_parquet(MODALITY_METRICS_PATH)

st.title("SECOP Monitor")
st.caption("MVP operativo para monitorear contratación pública en SECOP I y SECOP II")

if contracts.empty:
    st.info("Todavía no hay contratos normalizados. Ejecuta primero el pipeline diario.")
    st.stop()

min_reference = contracts["reference_date"].min()
max_reference = contracts["reference_date"].max()

with st.sidebar:
    st.header("Filtros")
    selected_sources = st.multiselect(
        "Fuente",
        options=sorted(contracts["source_system"].dropna().unique().tolist()),
        default=sorted(contracts["source_system"].dropna().unique().tolist()),
    )
    text_filter = st.text_input("Buscar en objeto contractual")
    default_start = max(min_reference, max_reference - pd.Timedelta(days=30))
    selected_dates = st.date_input(
        "Ventana de fechas",
        value=(default_start.date(), max_reference.date()),
        min_value=min_reference.date(),
        max_value=max_reference.date(),
    )

filtered = contracts.copy()
if selected_sources:
    filtered = filtered[filtered["source_system"].isin(selected_sources)]
if text_filter:
    filtered = filtered[
        filtered["contract_object"].fillna("").str.contains(text_filter, case=False, na=False)
    ]
if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
    start_date = pd.Timestamp(selected_dates[0], tz="UTC")
    end_date = pd.Timestamp(selected_dates[1], tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    filtered = filtered[
        filtered["reference_date"].notna()
        & (filtered["reference_date"] >= start_date)
        & (filtered["reference_date"] <= end_date)
    ]

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Contratos", f"{len(filtered):,}".replace(",", "."))
kpi2.metric("Monto total", format_currency(float(filtered["amount_value"].fillna(0).sum())))
kpi3.metric("Entidades", f"{filtered['entity_name'].nunique(dropna=True):,}".replace(",", "."))
kpi4.metric("Proveedores", f"{filtered['supplier_name'].nunique(dropna=True):,}".replace(",", "."))

chart_col, table_col = st.columns((1, 1))

with chart_col:
    st.subheader("Entidades con mayor monto")
    if not filtered.empty:
        top_entities = (
            filtered.groupby("entity_name", dropna=True)["amount_value"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        figure = px.bar(top_entities, x="amount_value", y="entity_name", orientation="h")
        figure.update_layout(yaxis_title="", xaxis_title="Monto")
        st.plotly_chart(figure, use_container_width=True)
    else:
        st.write("Sin datos para los filtros actuales.")

with table_col:
    st.subheader("Modalidades principales")
    if not filtered.empty:
        modality_view = (
            filtered.groupby("modality", dropna=True)
            .agg(contratos=("contract_uid", "count"), monto=("amount_value", "sum"))
            .reset_index()
            .sort_values("monto", ascending=False)
            .head(10)
        )
        modality_view["monto"] = modality_view["monto"].fillna(0).map(format_currency)
        st.dataframe(modality_view, use_container_width=True, hide_index=True)
    else:
        st.write("Sin datos para los filtros actuales.")

st.subheader("Contratos recientes")
recent_columns = [
    "reference_date",
    "source_system",
    "entity_name",
    "supplier_name",
    "amount_value",
    "modality",
    "contract_object",
]
recent_view = filtered[recent_columns].copy().sort_values("reference_date", ascending=False).head(100)
recent_view["reference_date"] = pd.to_datetime(recent_view["reference_date"], utc=True).dt.strftime("%Y-%m-%d")
recent_view["amount_value"] = recent_view["amount_value"].fillna(0).map(format_currency)
recent_view = recent_view.rename(
    columns={
        "reference_date": "Fecha",
        "source_system": "Fuente",
        "entity_name": "Entidad",
        "supplier_name": "Proveedor",
        "amount_value": "Valor",
        "modality": "Modalidad",
        "contract_object": "Objeto",
    }
)
st.dataframe(recent_view, use_container_width=True, hide_index=True)

with st.expander("Métricas precalculadas"):
    left, right = st.columns(2)
    with left:
        st.write("Resumen diario")
        st.dataframe(daily_metrics, use_container_width=True, hide_index=True)
        st.write("Top entidades")
        st.dataframe(entity_metrics.head(20), use_container_width=True, hide_index=True)
    with right:
        st.write("Top modalidades")
        st.dataframe(modality_metrics.head(20), use_container_width=True, hide_index=True)

with st.expander("Reportes generados"):
    reports = list_reports()
    if not reports:
        st.write("Aún no hay reportes HTML generados.")
    else:
        for report in reports:
            st.write(report.relative_to(BASE_DIR).as_posix())
