# Pruebas

`pytest` con una SparkSession local (`conftest.py`). Cada test arma sus propios datos chicos en una carpeta temporal: Landing nunca se toca.

| Archivo | Qué prueba |
|---|---|
| `test_quality_rules.py` | Reglas B02–B04 y S01–S03 con casos inválidos armados a propósito; que gane la primera regla que falla; tolerancia exacta de −0.01; imputación de `unit` y normalización |
| `test_ingesta.py` | Bronze batch: fila corrupta (B01), clave nula (B02) y duplicado. Bronze streaming: reentrega de un `event_id`, evento tardío, evento sin id a quarantine y re-ejecución con checkpoint (0 filas nuevas) |
| `test_silver_gold.py` | v1 queda con `null` (no 0) en `carbon_kg`/`genai_tokens`; MAD por grupo; grano único y sumas del mart; ranking de 14 días |
| `test_serving.py` | El script `cql/01_schema.cql` se parte en 2 sentencias con las PK esperadas |

```bash
python -m pytest -q tests          # desde la raíz del repo (en Colab: !cd /content/cloud-provider-analytics && ...)
```

Última ejecución: `evidence/02_tests_pytest.txt` (11 passed).

Pendiente para la final: test de conversión a USD (`test_fx`) cuando exista `revenue_by_org_month`.
