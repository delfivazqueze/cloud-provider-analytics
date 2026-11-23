# Pruebas

`pytest` con una SparkSession local (`conftest.py`). Cada test arma sus propios datos chicos en una carpeta temporal: Landing nunca se toca.

| Archivo | Qué prueba |
|---|---|
| `test_quality_rules.py` | Reglas B02–B04 y S01–S03 con casos inválidos armados a propósito; que gane la primera regla que falla; corrección de signo de costos negativos con consumo (D29); imputación de `unit` y normalización |
| `test_idempotencia_fallos_serving.py` | Pipeline de punta a punta (Landing → Bronze → Silver → Gold) ejecutado dos veces sobre un lake de prueba con reentrega de eventos: mismos conteos, costo y grano único; JSON corrupto a quarantine; archivo faltante registrado como `ERROR` en el run_log; conciliación fallida; carga a Cassandra por lotes con errores y conteo por partición (sesión simulada) |
| `test_ingesta.py` | Bronze batch: fila corrupta (B01), clave nula (B02) y duplicado. Bronze streaming: reentrega de un `event_id`, evento tardío, evento sin id a quarantine y re-ejecución con checkpoint (0 filas nuevas) |
| `test_silver_gold.py` | v1 queda con `null` (no 0) en `carbon_kg`/`genai_tokens`; MAD por grupo; grano único y sumas del mart; ranking de 14 días |
| `test_marts_finales.py` | Conversión a USD con una factura de cada moneda (USD con fx ≠ 1, ARS, EUR, créditos nulos); CSAT fuera de rango; serie diaria de Q3 sin huecos y con `all`; GenAI (tokens, costo por 1k) y contexto de `cost_anomaly_mart` |
| `test_serving.py` | El script `cql/01_schema.cql` se parte en 5 sentencias con las PK esperadas y nombres calificados por keyspace |

```bash
python -m pytest -q tests          # desde la raíz del repo (en Colab: !cd /content/cloud-provider-analytics && ...)
```

Última ejecución: `evidence/final_tests_pytest.txt` (21 passed).
