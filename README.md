# SECOP Monitor

MVP inicial para monitorear contratación pública desde SECOP I y SECOP II.

## Qué hace hoy

- descarga fuentes públicas reales desde datos.gov.co por defecto
- usa SECOP I histórico con contratos en `qddk-cgux`
- usa SECOP II contratos electrónicos en `ay65-guja`
- también admite archivos locales o URLs personalizadas
- normaliza SECOP I y SECOP II a un esquema común
- conserva una ventana operativa de últimos 180 días
- genera datasets derivados de 7, 30 y 180 días en Parquet
- produce métricas agregadas para el dashboard
- genera reportes HTML semanales y mensuales
- expone un dashboard simple en Streamlit
- genera reportes temáticos con búsqueda semántica usando Sentence Transformers

## Estructura

- [app/streamlit_app.py](app/streamlit_app.py): dashboard
- [pipeline/main.py](pipeline/main.py): orquestación principal
- [pipeline/sources.py](pipeline/sources.py): descarga de fuentes
- [pipeline/schema.py](pipeline/schema.py): normalización de columnas
- [pipeline/reconcile.py](pipeline/reconcile.py): deduplicación, retención y métricas
- [pipeline/reporting.py](pipeline/reporting.py): reportes HTML
- [pipeline/semantic_engine.py](pipeline/semantic_engine.py): motor semántico compartido
- [.github/workflows/daily_pipeline.yml](.github/workflows/daily_pipeline.yml): ejecución diaria

## Primer uso

1. Crear entorno virtual e instalar dependencias.
2. Copiar [.env.example](.env.example) solo si quieres cambiar defaults.
3. Opcional: definir `SOCRATA_APP_TOKEN` para mejorar límites de la API.
4. Ejecutar el pipeline:
   - `python -m pipeline.main daily`
5. Levantar la app:
   - `streamlit run app/streamlit_app.py`

Por defecto, el proyecto ya apunta a estos datasets públicos:

- SECOP I: `qddk-cgux`
- SECOP II: `ay65-guja`

Si prefieres otra fuente, puedes cambiar `SECOP_I_DATASET_ID`, `SECOP_II_DATASET_ID`, `SECOP_I_URL` o `SECOP_II_URL`.

## Búsqueda semántica

La parte semántica del pipeline quedó alineada con el buscador de [main.py](../Buscador_contratos/main.py), pero adaptada a ejecución batch.

El mismo motor semántico se reutiliza también en el dashboard de [app/streamlit_app.py](app/streamlit_app.py) para búsquedas temáticas interactivas sobre contratos recientes.

Usa por defecto:

- modelo `paraphrase-multilingual-MiniLM-L12-v2`
- umbral `SEMANTIC_SIMILARITY_MIN=0.25`
- máximo `SEMANTIC_TOP_K=250`

Puedes generar un reporte temático con:

- `python -m pipeline.main semantic-report --query "salud pública"`

## Pendientes importantes

- validar si estos datasets cubren exactamente el universo que quieres monitorear
- afinar la llave canónica de actualización para casos límite
- calibrar el umbral semántico según tus temas de monitoreo
- decidir despliegue final del dashboard

## Esquema canónico actual

Cada contrato normalizado intenta producir estos campos:

- `source_system`
- `contract_uid`
- `process_uid`
- `published_at`
- `updated_at`
- `contract_date`
- `entity_name`
- `supplier_name`
- `amount_value`
- `currency`
- `status`
- `modality`
- `contract_object`
- `region`
- `city`
- `source_url`
- `ingestion_date`
- `record_hash`
- `reference_date`

## Nota

Este arranque está hecho para mantener el proyecto muy simple. La parte más sensible será ajustar bien la identidad del contrato y el mapping real de SECOP.
