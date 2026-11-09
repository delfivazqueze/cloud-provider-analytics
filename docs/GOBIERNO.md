# Gobierno del dato (preliminar · 2ª entrega)

Qué controles existen hoy, quién responde por cada dataset, cómo se rastrea un dato desde Cassandra hasta el archivo de origen y quién puede leer qué. Todo lo que figura acá está implementado en `src/` y se ve en el notebook 02; lo pendiente está en `docs/BACKLOG.md`.

## 1. Matriz de gobierno

| Dataset | Zona | Dueño (responsable) | Quién escribe | Quién lee | Clasificación | Controles | Retención |
|---|---|---|---|---|---|---|---|
| Archivos originales | Landing | Cayetana Peces | sistemas fuente | jobs de ingesta | interna | solo lectura; nunca se modifica | 90 días |
| `customers_orgs`, `resources`, `support_tickets`, `marketing_touches`, `nps_surveys`, `billing_monthly` | Bronze | Cayetana Peces | `bronze_batch` | jobs de Silver, auditoría | interna | B01, B02, dedupe por clave natural | 1 año |
| `users` | Bronze | Cayetana Peces | `bronze_batch` | auditoría | **dato personal** (`email`) | no se propaga a Silver, Gold ni Cassandra | 1 año |
| `usage_events` | Bronze | Cayetana Peces | `bronze_stream` | jobs de Silver | interna | B02–B04, dedupe por `event_id`, checkpoint | 1 año |
| `dim_org` | Silver | Abril Solé | `silver_dim_org` | jobs de Silver/Gold | interna | tipos explícitos, NPS en [−100, 100] | 2 años |
| `usage_events` | Silver | Abril Solé | `silver_usage_events` | jobs de Gold, analistas | interna | S01–S03, conciliación Bronze = Silver + quarantine | 2 años |
| `org_daily_usage_by_service`, `top_services_14d` | Gold | Agustina Edalian | `gold_*` | carga a Cassandra, FinOps | interna | grano único, Σ costo Silver = Σ costo Gold | 3 años |
| Tablas de `cloud_analytics` | Serving | Agustina Edalian | notebook 02 §8 (token de la app) | FinOps (CQL) | interna | upsert por PK, conteo antes/después | mientras exista el mart |
| `quarantine/*` | Quarantine | Abril Solé | validadores | data stewards (grupo) | interna | `rule_id` + `motivo` por fila | 30 días desde que se resuelve |
| `_meta/run_log`, `_meta/anomaly_params`, `_checkpoints` | Meta | Delfina Vázquez Escati | todos los jobs | grupo | interna | append; los checkpoints solo se borran en un reinicio controlado | mientras el pipeline exista |

## 2. Reglas de calidad activas

| rule_id | Zona | Regla | Acción | Resultado con el dataset |
|---|---|---|---|---|
| B01_FILA_CORRUPTA | Bronze (CSV) | la fila respeta el esquema | quarantine | 0 (detectó 211 filas de `resources` antes de corregir el `escape`, D21) |
| B02_CLAVE_NULA | Bronze | clave natural no nula (`event_id`, `org_id`, `invoice_id`…) | quarantine | 0 |
| B03_TIMESTAMP_INVALIDO | Bronze (eventos) | `timestamp` parseable | quarantine | 0 |
| B04_SCHEMA_VERSION | Bronze (eventos) | `schema_version ∈ {1, 2}` | quarantine | 0 |
| — (dedupe) | Bronze | `event_id` único | se descarta el duplicado (estado acotado por watermark) | 0 duplicados en la muestra; probado con reentrega en `tests/test_ingesta.py` |
| S01_COSTO_NEGATIVO | Silver | `cost_usd_increment >= -0.01` | quarantine + `is_cost_anomaly = true`; entre −0.01 y 0 se lleva a 0 (`is_cost_adjusted`) | **211** |
| S02_UNIT_NO_IMPUTABLE | Silver | `unit` no nulo cuando hay `value` | se imputa desde `metric` (`unit_imputed`); si la métrica no es conocida, quarantine | 2.033 imputadas, 0 a quarantine |
| S03_ORG_INEXISTENTE | Silver | `org_id` existe en `dim_org` | quarantine | 0 |
| (flag) | Silver `dim_org` | NPS en [−100, 100] | se pone `null` + `is_nps_out_of_range` | 1 (NPS = 101) |

Las reglas con 0 casos en el dataset igual se prueban con datos armados a propósito en `tests/` (Landing no se toca).

## 3. Linaje

```
landing/usage_events_stream/events_part_NNNN.jsonl
   └─(bronze_stream, checkpoint _checkpoints/bronze_usage_events)─▶ bronze/usage_events [event_date]
        └─(silver_usage_events + dim_org)─▶ silver/usage_events [event_date]   ─▶ quarantine/usage_events_silver [rule_id]
             └─(gold_org_daily_usage)─▶ gold/org_daily_usage_by_service [usage_date]
                  ├─(serving)─▶ cloud_analytics.org_daily_usage_by_service   (Q1)
                  └─(gold_top_services_14d)─▶ gold/top_services_14d ─(serving)─▶ cloud_analytics.top_services_14d (Q2)

landing/customers_orgs.csv ─▶ bronze/customers_orgs [ingest_date] ─▶ silver/dim_org
```

Cómo se rastrea una fila: `run_id` está en todas las zonas y en Cassandra; con él se encuentra la ejecución en `_meta/run_log`. Silver y Bronze conservan `source_file` (archivo de Landing) e `ingest_ts`, y `value_raw` guarda el valor original antes del casteo.

## 4. Metadatos

- **Técnicos:** `ingest_ts`, `ingest_date`, `source_file`, `run_id`, `schema_version` en Bronze y Silver; `_meta/run_log` con filas de entrada, válidas, quarantine, duplicadas, duración y estado de cada job; `_meta/anomaly_params` con la mediana y el MAD de cada grupo usado para marcar anomalías.
- **De negocio:** `docs/DICCIONARIO.md` (columnas, tipos, unidades y dueño).
- **Esquemas:** versionados en código (`src/common/schemas.py`), una sola definición para batch y streaming.

## 5. Acceso y seguridad

- El token de AstraDB se genera desde el portal con rol *Database Administrator* **solo para la base `big_data`**, se guarda en los *Secrets* de Colab y nunca en el repo. El bundle (`secure-connect-*.zip`) está en el `.gitignore`.
- Mínimo privilegio: los jobs escriben en su zona; los analistas de FinOps solo consultan Cassandra (para la final: token de solo lectura para consumo).
- `users.email` es dato personal: queda en Bronze y no se usa en ninguna zona posterior.

## 6. Observabilidad

- `_meta/run_log`: un registro por job y ejecución. El estado pasa a `CONCILIACION_FALLIDA` si entrada ≠ válidas + quarantine + duplicados (objetivo O2).
- Streaming: `recentProgress` / `lastProgress` de cada query (filas por lote, duración, `numRowsDroppedByWatermark`, filas en estado), guardado en `evidence/02_resumen.json`.
