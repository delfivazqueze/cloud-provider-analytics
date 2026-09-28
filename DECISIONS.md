# Registro de decisiones técnicas

Formato: decisión · alternativas consideradas · motivo · estado. Cada decisión se revisa con el feedback de cada entrega.

| # | Fecha | Decisión | Alternativas | Motivo | Estado |
|---|---|---|---|---|---|
| D01 | 05/10/2026 | Patrón **Lambda** con una sola base de código Spark | Batch puro, Kappa | Los eventos exigen frescura. Los maestros y la facturación son batch por naturaleza. Cada archivo de eventos abarca 60 días (late data), y el recálculo batch corrige la capa speed. Spark usa la misma API en los dos caminos | Aceptada |
| D02 | 05/10/2026 | Zonas Landing / Bronze / Silver / Gold + Quarantine | Solo raw + curated | Lo exige la consigna y separa responsabilidades: inmutabilidad, tipado, conformado, consumo | Aceptada |
| D03 | 05/10/2026 | **Parquet snappy** en Bronze, Silver y Gold | CSV, JSON, Avro, ORC | Columnar, con compresión (medida ≈5× vs JSON), esquema embebido y lectura selectiva de columnas | Aceptada |
| D04 | 05/10/2026 | Eventos particionados **solo por `event_date`** | `event_date` + `service` | Las consultas filtran por fecha. Sumar `service` daría 360 carpetas de pocos KB (small files) | Aceptada · revisar si un día supera 128 MB |
| D05 | 05/10/2026 | Maestros en Bronze particionados por `ingest_date` (snapshot) | Overwrite sin historia | Permite reconstruir el estado de cualquier día y habilita un SCD2 | Aceptada |
| D06 | 05/10/2026 | **Esquemas explícitos** (sin `inferSchema`) | Inferencia | En streaming no se puede inferir, y en batch la inferencia oculta los problemas de tipos | Aceptada |
| D07 | 05/10/2026 | `value` se lee como string (`value_raw`) y se castea a `value_num` con fallback | Castear en la lectura | 1.309 valores llegan como texto; conservar el original da trazabilidad | Aceptada |
| D08 | 05/10/2026 | `unit` faltante se **imputa** a partir de `metric` (+ flag `unit_imputed`) | Quarantine | La relación métrica→unidad es 1:1 en 41.125 eventos: la imputación es determinística y no pierde 2.038 eventos | Propuesta · validar con el docente |
| D09 | 05/10/2026 | Anomalías de costo con **MAD por (servicio, métrica)** | z-score global, percentil global | Con umbral global, z-score marca 70 eventos y MAD marca el 25 %. El costo depende del servicio y la métrica | Propuesta · calibrar en la 2ª entrega |
| D10 | 05/10/2026 | Idempotencia: dedupe por clave natural + overwrite dinámico por partición + upsert en Cassandra + checkpoints | Append puro | Re-ejecutar no duplica (demostrado en Bronze) | Aceptada |
| D11 | 05/10/2026 | `spark.sql.shuffle.partitions = 8` | 200 (default) | Con ~13 MB, 200 particiones crean tareas vacías y archivos diminutos | Aceptada · subir al escalar |
| D12 | 05/10/2026 | `revenue_usd = (subtotal − coalesce(credits,0) + taxes) × exchange_rate_to_usd` | Dividir por el tipo de cambio | El tipo de cambio de ARS (≈0,0015) confirma que el campo es "USD por unidad de moneda" | Propuesta |
| D13 | 05/10/2026 | Serving **query-first**: una tabla por consulta; el Top-N se precalcula en Spark | Modelo relacional normalizado | Cassandra no hace joins ni ordena por agregados | Preliminar · se cierra en la clase del 02/11 |
| D14 | 05/10/2026 | Lake y checkpoints en Google Drive montado | Disco efímero de Colab | Colab borra `/content` al reiniciar y se perderían los offsets del stream | Propuesta |

## Decisiones abiertas
- Valor del watermark del streaming.
- SCD1 vs SCD2 para `dim_org`.
- Qué hacer con las facturas en USD cuyo tipo de cambio es distinto de 1.
- Escala de CSAT (se observa 0–7).
- Q3 global por severidad vs por organización.
- Anomalías como capacidad analítica equivalente vs un modelo de Spark ML.
