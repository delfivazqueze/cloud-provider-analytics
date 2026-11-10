# Backlog para el MVP final (07/12/2026)

Priorizado con MoSCoW después de la 2ª entrega. Regla que nos pusimos: nada de lo "deseable" se empieza hasta que lo "obligatorio" corra end-to-end desde un entorno limpio.

## Obligatorio (lo pide la consigna para la final)

| # | Tarea | Responsable | Estimación | Evidencia esperada |
|---|---|---|---|---|
| M1 | Silver de `billing_monthly` en USD (D12) + mart `revenue_by_org_month` + tabla y consulta **Q4** | Abril / Agustina | 8 h | CQL + captura; test de conversión ARS/EUR/USD |
| M2 | Silver de `support_tickets` + mart `tickets_by_org_date` + tabla y consulta **Q3** (cerrar escala de CSAT y Q3 global vs por org) | Abril / Agustina | 8 h | CQL + captura |
| M3 | Mart `genai_tokens_by_org_date` (tokens y costo estimado) + consulta **Q5** | Agustina | 5 h | CQL + captura |
| M4 | `cost_anomaly_mart` (org, fecha, servicio) a partir de `is_cost_anomaly` + métricas del componente analítico | Abril | 4 h | tabla Gold + resumen de tasas |
| M5 | Capa speed hasta Gold/Cassandra con `foreachBatch` (upsert solo de las claves del micro-lote) para cumplir O1 | Cayetana | 8 h | `lastProgress` + fila visible en Cassandra < 10 min |
| M6 | Diccionario y gobierno completos para los 5 marts; token de solo lectura para consumo | Delfina | 4 h | `docs/` actualizados |
| M7 | Quickstart validado desde un Colab nuevo por alguien que no armó el pipeline | todo el grupo | 2 h | checklist firmada en el README |
| M8 | Presentación ejecutiva, video y defensa oral con participación de las 4 | todo el grupo | 12 h | slides + video |

## Deseable (si queda tiempo, sin poner en riesgo lo anterior)

| # | Tarea | Por qué |
|---|---|---|
| S1 | Compactar Bronze `usage_events` (hoy 4 archivos por fecha) | small files; hoy no impacta pero crece con cada micro-lote |
| S2 | SCD tipo 2 para `dim_org` desde los snapshots de Bronze (D22) | historia de cambios de plan |
| S3 | Silver de `resources` parseando `tags_json` (ya se lee bien desde D21) | enriquecer eventos con entorno/PII |
| S4 | Spark Cassandra Connector en lugar del driver (D18) | escritura distribuida al escalar |
| S5 | Tests de los jobs completos sobre una muestra del dataset (no solo de funciones) | más confianza en la integración |

## Fuera de alcance

| Tarea | Motivo |
|---|---|
| Modelo supervisado de Spark ML (churn, forecasting de costos) | no hay variable objetivo confiable; el componente analítico se cubre con anomalías (D20) |
| Kafka como fuente del stream | la consigna define el directorio JSONL como fuente |
| Orquestación con Airflow y despliegue en clúster | el MVP corre en Colab; se menciona como evolución en la arquitectura |
| `marketing_touches` y `nps_surveys` más allá de Bronze | no alimentan ninguna de las 5 consultas obligatorias |
