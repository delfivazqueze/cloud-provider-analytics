# Diccionario de datos (Silver, Gold y Serving)

Bronze tiene las mismas columnas que la fuente (ver `src/common/schemas.py`) más `ingest_ts`, `ingest_date`, `source_file` y `run_id`. Acá se documentan las tablas que consume alguien.

## silver/usage_events
Grano: 1 fila = 1 evento de uso válido. Partición: `event_date`. Dueño: Abril Solé.

| Columna | Tipo | Descripción |
|---|---|---|
| event_id | string | Clave natural del evento (única) |
| event_ts / event_date | timestamp / date | Momento del evento (UTC) y su fecha |
| org_id, org_name, industry, plan_tier, is_enterprise | string / boolean | Organización y atributos de `dim_org` (join broadcast) |
| resource_id, service, region, metric | string | Recurso y servicio, normalizados a minúsculas |
| value_raw | string | Valor tal como llegó (a veces texto) |
| value_num | double | `value_raw` casteado; null si no castea o no vino |
| unit | string | Unidad (`count`, `hours`, `gb_hours`) |
| unit_imputed | boolean | La unidad se completó desde `metric` (regla S02) |
| cost_usd_increment | double | Costo incremental en USD; valores entre −0.01 y 0 llevados a 0 |
| is_cost_adjusted | boolean | El costo se llevó a 0 por la tolerancia |
| mad_score | double | 0.6745·\|costo − mediana\| / MAD de su (service, metric) |
| is_cost_anomaly | boolean | `mad_score > 3.5` |
| requests, cpu_hours, storage_gb_hours | double | `value_num` según la métrica del evento (null en las otras) |
| genai_tokens | bigint | Solo v2 y servicio `genai`; null en el resto |
| carbon_kg | double | Solo v2; null en v1 (no medido ≠ 0) |
| schema_version | int | 1 o 2 |
| ingest_ts, source_file, run_id | | Linaje |

## silver/dim_org
Grano: 1 fila = 1 organización (SCD1). Dueño: Abril Solé.

| Columna | Tipo | Descripción |
|---|---|---|
| org_id | string | Clave |
| org_name, industry, sales_rep | string | Atributos del CRM |
| hq_region, plan_tier, lifecycle_stage, marketing_source | string | En minúsculas |
| is_enterprise | boolean | `"True"/"False"` del CSV convertido |
| signup_date | date | Fecha de alta |
| nps_score | double | NPS en [−100, 100]; null si venía fuera de rango |
| is_nps_out_of_range | boolean | El NPS original estaba fuera de rango |
| source_file, updated_ts, run_id | | Linaje |

## gold/org_daily_usage_by_service
Grano: **(org_id, usage_date, service)**. Partición: `usage_date`. Dueño: Agustina Edalian.

| Columna | Tipo | Unidad | Cálculo |
|---|---|---|---|
| org_id, org_name | string | | |
| usage_date | date | | `event_date` |
| service | string | | |
| daily_cost_usd | double | USD | Σ `cost_usd_increment` (4 decimales) |
| requests | double | requests | Σ `requests` |
| cpu_hours | double | horas | Σ `cpu_hours` |
| storage_gb_hours | double | GB·hora | Σ `storage_gb_hours` |
| genai_tokens | bigint | tokens | Σ `genai_tokens` (null si el día no tiene) |
| carbon_kg | double | kg CO₂ | Σ `carbon_kg` (null en días v1) |
| events | bigint | eventos | cantidad de eventos válidos |
| anomaly_events | bigint | eventos | cantidad con `is_cost_anomaly` |
| run_id | string | | ejecución que lo generó |

## gold/top_services_14d
Grano: **(org_id, as_of_date, rank)**. Solo fechas con 14 días completos de historia (desde 2025-07-16).

| Columna | Tipo | Descripción |
|---|---|---|
| org_id | string | |
| as_of_date | date | Último día de la ventana |
| rank | int | 1 = servicio de mayor costo en la ventana |
| service | string | |
| cost_14d_usd | double | Σ `daily_cost_usd` de `as_of_date − 13` a `as_of_date` |
| requests_14d | double | Σ `requests` en la misma ventana |

## Serving (keyspace `cloud_analytics`)

| Tabla | PRIMARY KEY | Consulta | Columnas |
|---|---|---|---|
| `org_daily_usage_by_service` | `((org_id), usage_date, service)` · `usage_date DESC` | Q1 | las del mart Gold |
| `top_services_14d` | `((org_id, as_of_date), rank)` · `rank ASC` | Q2 | las de `gold/top_services_14d` + `run_id` |

## quarantine/*
Mismas columnas que la fila original + `rule_id` (partición) + `motivo`. En `usage_events_silver` además `is_cost_anomaly`.

## _meta/run_log
`run_id, job, zona, dataset, filas_entrada, filas_validas, filas_quarantine, filas_duplicadas, duracion_s, estado, detalle, ts`.
