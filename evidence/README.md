# Evidencias de ejecución

| Archivo | Entrega | Qué demuestra |
|---|---|---|
| `01_profile_summary.json` | 1ª | Conteos, nulos, evolución de esquema, costos negativos, percentiles, tamaños de Bronze |
| `01_event_time_range_by_file.csv` | 1ª | Rango temporal de cada archivo de eventos (late data) |
| `02_resumen.json` | 2ª | Resumen de la corrida: streaming, conteos por zona, calidad, anomalías, conciliación de Gold e idempotencia |
| `02_run_log.csv` | 2ª | `_meta/run_log` de la corrida y de la re-ejecución (filas de entrada, válidas, quarantine, duplicadas, estado) |
| `02_watermark_experimento.csv` | 2ª | Eventos descartados por tardíos con watermark de 2 y 60 días (D15) |
| `02_quarantine_muestras.csv` | 2ª | Muestras de quarantine por regla, con `rule_id` y `motivo` |
| `02_anomalias_por_grupo.csv` | 2ª | Mediana, MAD, eventos y tasa de anomalías por (servicio, métrica) |
| `02_gold_muestra.csv` | 2ª | Primeras 200 filas del mart `org_daily_usage_by_service` |
| `02_lake_stats.csv` | 2ª | Particiones, archivos y tamaño por dataset y zona |
| `02_tests_pytest.txt` | 2ª | Salida de `pytest` (11 tests) |
| `02_cassandra_q1.csv`, `02_cassandra_q2.csv` | 2ª | Resultados de Q1 y Q2 leídos desde AstraDB |
| `final_resumen.json` | Final | Resumen de la corrida final: streaming, conteos por zona, calidad, anomalías, conciliaciones de Gold, facturación, tickets e idempotencia |
| `final_run_log.csv` | Final | `_meta/run_log` de la corrida y de la re-ejecución |
| `final_watermark_experimento.csv` | Final | Eventos descartados por tardíos con watermark de 2 y 60 días |
| `final_quarantine_muestras.csv` | Final | Muestras de quarantine por regla |
| `final_anomalias_por_grupo.csv` | Final | Mediana, MAD y tasa de anomalías por (servicio, métrica) |
| `final_gold_*_muestra.csv` | Final | Primeras 200 filas de cada mart Gold |
| `final_lake_stats.csv` | Final | Particiones, archivos y tamaño por dataset y zona |
| `final_tests_pytest.txt` | Final | Salida de `pytest` (21 tests) |
| `final_cassandra_q1.csv` … `final_cassandra_q5.csv` | Final | Resultados de Q1–Q5 leídos desde AstraDB |
| `capturas/` | 1ª, 2ª y final | Capturas de Colab y de la CQL Console de AstraDB |
