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
| cost_usd_increment | double | Costo incremental en USD (≥ 0; signo corregido si venía negativo con consumo, D29) |
| cost_usd_raw | double | Costo tal como llegó en el evento |
| is_cost_sign_corrected | boolean | El costo venía negativo con consumo medido y se corrigió el signo |
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

## silver/billing_usd
Grano: 1 fila = 1 factura (una por org y mes en el dataset). Partición: `billing_month`. Dueño: Abril Solé.

| Columna | Tipo | Descripción |
|---|---|---|
| invoice_id | string | Clave |
| org_id, org_name | string | Organización |
| billing_month / billing_month_date | string (`yyyy-MM`) / date | Mes facturado |
| currency | string | USD, ARS o EUR (mayúsculas) |
| subtotal, credits, taxes | double | Montos en la moneda original; `credits` nulo → 0 |
| is_credits_missing | boolean | `credits` venía nulo |
| exchange_rate_to_usd | double | Tipo de cambio original del archivo |
| fx_applied | double | Tipo de cambio usado: 1 para USD (D23), el original para ARS y EUR |
| is_fx_overridden | boolean | Factura en USD cuyo tipo de cambio original no era 1 |
| is_adjustment | boolean | Subtotal negativo (nota de crédito o ajuste) |
| subtotal_usd, credits_usd, taxes_usd | double | Montos × `fx_applied` |
| revenue_usd | double | (subtotal − credits + taxes) × `fx_applied` |
| source_file, ingest_ts, run_id | | Linaje |

## silver/support_tickets
Grano: 1 fila = 1 ticket. Dueño: Abril Solé.

| Columna | Tipo | Descripción |
|---|---|---|
| ticket_id | string | Clave |
| org_id, org_name | string | Organización |
| category, severity | string | En minúsculas; severidad: critical, high, medium, low |
| is_critical | boolean | `severity = 'critical'` |
| created_date / resolved_date | date | Alta y resolución |
| is_open | boolean | Sin fecha de resolución |
| resolution_days | int | Días entre alta y resolución |
| sla_breached | boolean | Se incumplió el SLA |
| csat | double | Satisfacción 1–5; null si no respondió o estaba fuera de rango |
| is_csat_out_of_range | boolean | El CSAT original estaba fuera de [1, 5] (D24) |
| source_file, ingest_ts, run_id | | Linaje |

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

## gold/revenue_by_org_month
Grano: **(org_id, billing_month)**. Partición: `billing_month`. Dueño: Agustina Edalian.

| Columna | Tipo | Descripción |
|---|---|---|
| org_id, org_name | string | |
| billing_month | string | `yyyy-MM` |
| invoices | bigint | Cantidad de facturas |
| currencies | string | Monedas de origen |
| subtotal_usd, credits_usd, taxes_usd, revenue_usd | double | Sumas en USD (2 decimales) |
| fx_applied | double | Tipo de cambio aplicado |
| adjustments | bigint | Facturas con subtotal negativo |
| fx_overridden | bigint | Facturas en USD con tipo de cambio llevado a 1 |

## gold/tickets_by_org_date
Grano: **(org_id, ticket_date, severity)**. Dueño: Agustina Edalian.

| Columna | Tipo | Descripción |
|---|---|---|
| org_id, org_name, severity | string | |
| ticket_date | date | Fecha de alta |
| tickets | bigint | Tickets creados |
| sla_breached | bigint | Tickets con SLA incumplido |
| sla_breach_rate | double | `sla_breached / tickets` |
| csat_avg / csat_responses | double / bigint | CSAT promedio (1–5) y cantidad de respuestas válidas |
| open_tickets | bigint | Tickets sin resolver |

## gold/tickets_by_severity_date
Grano: **(severity, ticket_date)**, con todos los días entre el primer y el último ticket. `severity` incluye `all` (todas las severidades). Es la vista para Q3 (D25).

| Columna | Tipo | Descripción |
|---|---|---|
| severity | string | critical, high, medium, low o all |
| ticket_date | date | |
| tickets, sla_breached | bigint | 0 los días sin tickets |
| sla_breach_rate | double | null los días sin tickets |
| csat_avg | double | |

## gold/genai_tokens_by_org_date
Grano: **(org_id, usage_date)**, días con eventos del servicio `genai`. Dueño: Agustina Edalian.

| Columna | Tipo | Unidad | Cálculo |
|---|---|---|---|
| genai_tokens | bigint | tokens | Σ `genai_tokens` (null antes de schema v2, 18/07) |
| genai_cost_usd | double | USD | Σ `cost_usd_increment` de los eventos genai (costo estimado, D27) |
| cost_per_1k_tokens_usd | double | USD | 1000 × costo / tokens |
| requests | double | requests | Σ `requests` |
| events | bigint | eventos | |

## gold/cost_anomaly_mart
Grano: **(org_id, usage_date, service)**. Partición: `usage_date`. Dueño: Abril Solé (método) y Agustina Edalian (mart).

| Columna | Tipo | Descripción |
|---|---|---|
| daily_cost_usd, events, anomaly_events | | Del mart de uso diario |
| anomalous_cost_usd | double | Costo de los eventos marcados como anómalos ese día |
| max_mad_score | double | Score MAD más alto del día |
| median_daily_cost_usd | double | Mediana del costo diario de esa org y servicio en el período |
| cost_vs_median | double | `daily_cost_usd / median_daily_cost_usd` |
| is_anomalous_day | boolean | El día tiene al menos un evento anómalo |
| sign_corrected_events | bigint | Eventos del día con el signo del costo corregido (D29) |

## Serving (keyspace `cloud_analytics`)

| Tabla | PRIMARY KEY | Consulta | Columnas |
|---|---|---|---|
| `org_daily_usage_by_service` | `((org_id), usage_date, service)` · `usage_date DESC` | Q1 | las del mart Gold |
| `top_services_14d` | `((org_id, as_of_date), rank)` · `rank ASC` | Q2 | las de `gold/top_services_14d` + `run_id` |
| `tickets_by_severity_date` | `((severity), ticket_date)` · `ticket_date DESC` | Q3 | las de `gold/tickets_by_severity_date` |
| `revenue_by_org_month` | `((org_id), billing_month)` · `billing_month DESC` | Q4 | las de `gold/revenue_by_org_month` (sin `fx_overridden`) |
| `genai_tokens_by_org_date` | `((org_id), usage_date)` · `usage_date DESC` | Q5 | las de `gold/genai_tokens_by_org_date` |

## quarantine/*
Mismas columnas que la fila original + `rule_id` (partición) + `motivo`. En `usage_events_silver` además `is_cost_anomaly`.

## _meta/run_log
`run_id, job, zona, dataset, filas_entrada, filas_validas, filas_quarantine, filas_duplicadas, duracion_s, estado, detalle, ts`.
