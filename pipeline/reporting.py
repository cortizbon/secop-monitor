from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
from jinja2 import Template

REPORT_TEMPLATE = Template(
    """
<!doctype html>
<html lang="es">
  <head>
    <meta charset="utf-8">
    <title>{{ title }}</title>
    <style>
      body {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
        margin: 40px;
        color: #17202a;
      }
      h1, h2, h3 { color: #0b4f6c; }
      .meta { color: #5d6d7e; margin-bottom: 24px; }
      .kpis {
        display: grid;
        grid-template-columns: repeat(4, minmax(120px, 1fr));
        gap: 12px;
        margin: 20px 0 28px;
      }
      .kpi {
        border: 1px solid #d6eaf8;
        border-radius: 8px;
        padding: 14px;
        background: #f8fbfd;
      }
      .kpi-label { font-size: 12px; text-transform: uppercase; color: #5d6d7e; }
      .kpi-value { font-size: 22px; font-weight: 700; margin-top: 6px; }
      table {
        width: 100%;
        border-collapse: collapse;
        margin: 16px 0 28px;
        font-size: 14px;
      }
      th, td {
        border-bottom: 1px solid #eaecee;
        padding: 10px 8px;
        text-align: left;
        vertical-align: top;
      }
      th { background: #f4f6f7; }
      .note {
        margin-top: 28px;
        font-size: 13px;
        color: #566573;
      }
    </style>
  </head>
  <body>
    <h1>{{ title }}</h1>
    <p class="meta">Generado: {{ generated_at }}{% if subtitle %} · {{ subtitle }}{% endif %}</p>

    <div class="kpis">
      {% for kpi in kpis %}
      <div class="kpi">
        <div class="kpi-label">{{ kpi.label }}</div>
        <div class="kpi-value">{{ kpi.value }}</div>
      </div>
      {% endfor %}
    </div>

    {% if narrative %}
    <h2>Resumen ejecutivo</h2>
    <p>{{ narrative }}</p>
    {% endif %}

    <h2>Top contratos</h2>
    {{ top_contracts_table }}

    <h2>Entidades con mayor monto</h2>
    {{ top_entities_table }}

    <h2>Modalidades principales</h2>
    {{ top_modalities_table }}

    {% if query %}
    <h2>Consulta semántica</h2>
    <p><strong>Consulta:</strong> {{ query }}</p>
    {% endif %}

    <p class="note">Reporte generado automáticamente a partir del dataset operativo de SECOP Monitor.</p>
  </body>
</html>
"""
)



def _format_currency(value: float) -> str:
    return f"${value:,.0f}".replace(",", ".")



def _safe_html_table(dataframe: pd.DataFrame) -> str:
    if dataframe.empty:
        return "<p>Sin datos disponibles para este periodo.</p>"
    return dataframe.to_html(index=False, border=0, escape=True)


def _amount_series(dataframe: pd.DataFrame) -> pd.Series:
    if dataframe.empty:
        return pd.Series(dtype="float64")
    return pd.to_numeric(dataframe["amount_value"], errors="coerce")



def build_summary(dataframe: pd.DataFrame) -> dict[str, str]:
    amounts = _amount_series(dataframe)
    total_amount = float(amounts.fillna(0).sum()) if not dataframe.empty else 0.0
    most_expensive = float(amounts.fillna(0).max()) if not dataframe.empty else 0.0
    return {
        "contracts": f"{len(dataframe):,}".replace(",", "."),
        "total_amount": _format_currency(total_amount),
        "entities": f"{dataframe['entity_name'].nunique(dropna=True):,}".replace(",", ".") if not dataframe.empty else "0",
        "most_expensive": _format_currency(most_expensive),
    }



def build_narrative(dataframe: pd.DataFrame, label: str) -> str:
    if dataframe.empty:
        return f"No se encontraron contratos para el periodo {label}."

    top_entity = (
        dataframe.groupby("entity_name", dropna=True)["amount_value"].sum().sort_values(ascending=False).head(1)
    )
    top_modality = dataframe["modality"].dropna().mode()
    entity_text = top_entity.index[0] if not top_entity.empty else "sin entidad dominante"
    modality_text = top_modality.iloc[0] if not top_modality.empty else "sin modalidad dominante"

    return (
        f"En el periodo {label} se registraron {len(dataframe):,} contratos por un valor acumulado de "
        f"{build_summary(dataframe)['total_amount']}. La entidad con mayor monto agregado fue {entity_text} "
        f"y la modalidad más frecuente fue {modality_text}."
    ).replace(",", ".")



def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "consulta"



def render_report(
    dataframe: pd.DataFrame,
    output_path: Path,
    *,
    title: str,
    subtitle: str,
    generated_at: str,
    query: str | None = None,
) -> Path:
    top_contracts = dataframe[
        ["reference_date", "entity_name", "supplier_name", "amount_value", "contract_object", "source_system"]
    ].copy() if not dataframe.empty else pd.DataFrame()
    if not top_contracts.empty:
        top_contracts = top_contracts.sort_values("amount_value", ascending=False).head(15)
        top_contracts["amount_value"] = pd.to_numeric(top_contracts["amount_value"], errors="coerce").fillna(0).map(_format_currency)
        top_contracts["reference_date"] = pd.to_datetime(top_contracts["reference_date"], utc=True).dt.strftime("%Y-%m-%d")
        top_contracts = top_contracts.rename(
            columns={
                "reference_date": "Fecha",
                "entity_name": "Entidad",
                "supplier_name": "Proveedor",
                "amount_value": "Valor",
                "contract_object": "Objeto",
                "source_system": "Fuente",
            }
        )

    entity_table = pd.DataFrame()
    modality_table = pd.DataFrame()
    if not dataframe.empty:
        entity_table = (
            dataframe.groupby("entity_name", dropna=True)
            .agg(Contratos=("contract_uid", "count"), Valor=("amount_value", "sum"))
            .reset_index()
            .rename(columns={"entity_name": "Entidad"})
            .sort_values("Valor", ascending=False)
            .head(10)
        )
        if not entity_table.empty:
          entity_table["Valor"] = pd.to_numeric(entity_table["Valor"], errors="coerce").fillna(0).map(_format_currency)

        modality_table = (
            dataframe.groupby("modality", dropna=True)
            .agg(Contratos=("contract_uid", "count"), Valor=("amount_value", "sum"))
            .reset_index()
            .rename(columns={"modality": "Modalidad"})
            .sort_values("Valor", ascending=False)
            .head(10)
        )
        if not modality_table.empty:
          modality_table["Valor"] = pd.to_numeric(modality_table["Valor"], errors="coerce").fillna(0).map(_format_currency)

    summary = build_summary(dataframe)
    html = REPORT_TEMPLATE.render(
        title=title,
        subtitle=subtitle,
        generated_at=generated_at,
        query=query,
        narrative=build_narrative(dataframe, subtitle),
        kpis=[
            {"label": "Contratos", "value": summary["contracts"]},
            {"label": "Monto total", "value": summary["total_amount"]},
            {"label": "Entidades", "value": summary["entities"]},
            {"label": "Contrato mayor", "value": summary["most_expensive"]},
        ],
        top_contracts_table=_safe_html_table(top_contracts),
        top_entities_table=_safe_html_table(entity_table),
        top_modalities_table=_safe_html_table(modality_table),
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    return output_path



def semantic_report_path(base_directory: Path, query: str, timestamp_slug: str) -> Path:
    return base_directory / f"semantic_{_slugify(query)}_{timestamp_slug}.html"
