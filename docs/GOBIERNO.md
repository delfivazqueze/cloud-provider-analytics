# Gobierno del dato

Responsables, controles de calidad, linaje, metadatos, seguridad y observabilidad del pipeline. Todo lo que figura acá está implementado en `src/` y se ve ejecutado en el notebook 03.

## 1. Matriz de gobierno

| Dataset | Zona | Dueño (responsable) | Quién escribe | Quién lee | Clasificación | Controles | Retención |
|---|---|---|---|---|---|---|---|
| Archivos originales | Landing | Cayetana Peces | sistemas fuente | jobs de ingesta | interna | solo lectura; nunca se modifica | 90 días |
| `customers_orgs`, `resources`, `support_tickets`, `marketing_touches`, `nps_surveys`, `billing_monthly` | Bronze | Cayetana Peces | `bronze_batch` | jobs de Silver, auditoría | interna | B01, B02, dedupe por clave natural | 1 año |
| `users` | Bronze | Cayetana Peces | `bronze_batch` | auditoría | **dato personal** (`email`) | no se propaga a Silver, Gold ni Cassandra | 1 año |
| `usage_events` | Bronze | Cayetana Peces | `bronze_stream` | jobs de Silver | interna | B02–B04, dedupe por `event_id`, checkpoint | 1 año |
| `dim_org` | Silver | Abril Solé | `silver_dim_org` | jobs de Silver/Gold | interna | tipos explícitos, NPS en [−100, 100] | 2 años |
| `usage_events` | Silver | Abril Solé | `silver_usage_events` | jobs de Gold, analistas | interna | S01–S03, conciliación Bronze = Silver + quarantine | 2 años |
| `billing_usd` | Silver | Abril Solé | `silver_billing_usd` | jobs de Gold, FinOps | **confidencial** (facturación) | S03–S05, `is_fx_overridden`, `is_adjustment` | 2 años |
| `support_tickets` | Silver | Abril Solé | `silver_support_tickets` | jobs de Gold, Soporte | interna | S03, S05, CSAT en [1, 5] | 2 años |
| `org_daily_usage_by_service`, `top_services_14d`, `cost_anomaly_mart` | Gold | Agustina Edalian | `gold_*` | carga a Cassandra, FinOps | interna | grano único, Σ costo Silver = Σ costo Gold | 3 años |
| `revenue_by_org_month` | Gold | Agustina Edalian | `gold_revenue_by_org_month` | carga a Cassandra, FinOps | **confidencial** | grano único, Σ revenue Silver = Σ revenue Gold | 3 años |
| `tickets_by_org_date`, `tickets_by_severity_date` | Gold | Agustina Edalian | `gold_tickets_*` | carga a Cassandra, Soporte | interna | grano único, Σ tickets Silver = Σ tickets Gold | 3 años |
| `genai_tokens_by_org_date` | Gold | Agustina Edalian | `gold_genai_tokens_by_org_date` | carga a Cassandra, Producto | interna | grano único, Σ tokens Silver = Σ tokens Gold | 3 años |
| Tablas de `cloud_analytics` (5) | Serving | Agustina Edalian | notebook 03 §8 (token de la app) | FinOps, Soporte, Producto (CQL) | interna / confidencial (revenue) | upsert por PK, conteo antes/después | mientras exista el mart |
| `quarantine/*` | Quarantine | Abril Solé | validadores | data stewards (grupo) | según la fuente | `rule_id` + `motivo` por fila | 30 días desde que se resuelve |
| `_meta/run_log`, `_meta/anomaly_params`, `_checkpoints` | Meta | Delfina Vázquez Escati | todos los jobs | grupo | interna | append; los checkpoints solo se borran en un reinicio controlado | mientras el pipeline exista |

## 2. Reglas de calidad

| rule_id | Zona | Regla | Acción | Resultado con el dataset |
|---|---|---|---|---|
| B01_FILA_CORRUPTA | Bronze (CSV) | la fila respeta el esquema | quarantine | 0 (detectó 211 filas de `resources` antes de corregir el `escape`, D21) |
| B02_CLAVE_NULA | Bronze | clave natural no nula (`event_id`, `org_id`, `invoice_id`…) | quarantine | 0 |
| B03_TIMESTAMP_INVALIDO | Bronze (eventos) | `timestamp` parseable | quarantine | 0 |
| B04_SCHEMA_VERSION | Bronze (eventos) | `schema_version ∈ {1, 2}` | quarantine | 0 |
| — (dedupe) | Bronze | `event_id` único | se descarta el duplicado (estado acotado por watermark) | 0 duplicados en la muestra; probado con reentrega en `tests/test_ingesta.py` |
| S01_COSTO_NEGATIVO_SIN_CONSUMO | Silver (eventos) | `cost_usd_increment >= 0` (D29) | con consumo (`value > 0`): se corrige el signo, `cost_usd_raw` + `is_cost_sign_corrected`; sin consumo: quarantine + `is_cost_anomaly = true` | **212 corregidos, 4 a quarantine** |
| S02_UNIT_NO_IMPUTABLE | Silver (eventos) | `unit` no nulo cuando hay `value` | se imputa desde `metric` (`unit_imputed`); si la métrica no es conocida, quarantine | 2.038 imputadas, 0 a quarantine |
| S03_ORG_INEXISTENTE | Silver (eventos, facturación, tickets) | `org_id` existe en `dim_org` | quarantine | 0 |
| S04_MONEDA_INVALIDA | Silver (facturación) | `currency ∈ {USD, ARS, EUR}` y `exchange_rate_to_usd > 0` | quarantine | 0 |
| S05_FECHA_INVALIDA | Silver (facturación, tickets) | mes de facturación / fecha de creación parseable | quarantine | 0 |
| (flag) | Silver `dim_org` | NPS en [−100, 100] | `null` + `is_nps_out_of_range` | 1 (NPS = 101) |
| (flag) | Silver `support_tickets` | CSAT en [1, 5] (D24) | `null` + `is_csat_out_of_range` | 40 |
| (flag) | Silver `billing_usd` | factura en USD con tipo de cambio 1 (D23) | se usa 1 + `is_fx_overridden` | 160 |

Las reglas con 0 casos en el dataset se prueban con datos armados a propósito en `tests/` (Landing no se toca).

**Conciliaciones automáticas:** Bronze = Silver + quarantine (eventos: 43.200 = 43.196 + 4); Σ costo Silver = Σ costo Gold (149.246,81 USD); Σ revenue Silver = Σ revenue Gold (164.293,06 vs 164.293,07 por redondeo a 2 decimales por fila); tickets Silver = tickets Gold (1.000); tokens Silver = tokens Gold (2.558.359); filas en Cassandra = filas en Gold.

## 3. Linaje

```
landing/usage_events_stream/*.jsonl ─(bronze_stream, checkpoint)─▶ bronze/usage_events [event_date]
   └─(silver_usage_events + dim_org)─▶ silver/usage_events [event_date] ─▶ quarantine/usage_events_silver [rule_id]
        ├─(gold)─▶ gold/org_daily_usage_by_service ─┬─▶ cloud_analytics.org_daily_usage_by_service   (Q1)
        │                                          ├─▶ gold/top_services_14d ─▶ cloud_analytics.top_services_14d (Q2)
        │                                          └─▶ gold/cost_anomaly_mart
        └─(gold)─▶ gold/genai_tokens_by_org_date ─────▶ cloud_analytics.genai_tokens_by_org_date     (Q5)

landing/billing_monthly.csv ─▶ bronze/billing_monthly [billing_month] ─▶ silver/billing_usd [billing_month]
   └─▶ gold/revenue_by_org_month [billing_month] ─▶ cloud_analytics.revenue_by_org_month             (Q4)

landing/support_tickets.csv ─▶ bronze/support_tickets [ingest_date] ─▶ silver/support_tickets
   └─▶ gold/tickets_by_org_date ─▶ gold/tickets_by_severity_date ─▶ cloud_analytics.tickets_by_severity_date (Q3)

landing/customers_orgs.csv ─▶ bronze/customers_orgs [ingest_date] ─▶ silver/dim_org (join en todo Silver)
```

Cómo se rastrea una fila: `run_id` está en todas las zonas y en Cassandra; con él se encuentra la ejecución en `_meta/run_log`. Bronze y Silver conservan `source_file` (archivo de Landing) e `ingest_ts`; `value_raw` guarda el valor original antes del casteo y `billing_usd` conserva el `exchange_rate_to_usd` original junto al `fx_applied`.

## 4. Metadatos

- **Técnicos:** `ingest_ts`, `ingest_date`, `source_file`, `run_id`, `schema_version` en Bronze y Silver; `_meta/run_log` con filas de entrada, válidas, quarantine, duplicadas, duración y estado de cada job; `_meta/anomaly_params` con la mediana y el MAD de cada grupo usado para marcar anomalías.
- **De negocio:** `docs/DICCIONARIO.md` (columnas, tipos, unidades, grano y dueño de cada tabla).
- **Esquemas:** versionados en código (`src/common/schemas.py`), una sola definición para batch y streaming. Las tablas de Cassandra, en `cql/01_schema.cql`.

## 5. Seguridad y acceso

- El token de AstraDB del pipeline tiene rol *Database Administrator* **solo para la base `big_data`**, se guarda en los *Secrets* de Colab y nunca en el repo. El bundle (`secure-connect-*.zip`) y `config/pipeline.yaml` están en el `.gitignore`.
- Mínimo privilegio: cada job escribe solo en su zona; los usuarios de consumo leen Cassandra con un token de rol de solo lectura, sin acceso al lake.
- Datos sensibles: `users.email` (dato personal) queda en Bronze y no se usa después; la facturación (`billing_usd`, `revenue_by_org_month`) se clasifica como confidencial.

## 6. Observabilidad

- `_meta/run_log`: un registro por job y ejecución. El estado pasa a `CONCILIACION_FALLIDA` si entrada ≠ válidas + quarantine + duplicados (objetivo O2), y a `ERROR` con el mensaje de la excepción si el job falla (D30). Copia en `evidence/final_run_log.csv`.
- Streaming: `recentProgress` / `lastProgress` de cada query (filas por lote, duración, `numRowsDroppedByWatermark`, filas en estado), guardado en `evidence/final_resumen.json`.
- Serving: filas enviadas y errores por tabla en cada carga, y conteo de Cassandra contra Gold.
