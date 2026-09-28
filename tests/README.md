# Pruebas (2ª entrega)

Pruebas previstas con `pytest` + SparkSession local:
- `test_schemas.py`: los esquemas leen una muestra sin filas corruptas.
- `test_quality_rules.py`: cada regla manda a quarantine los casos inválidos conocidos (event_id nulo, costo < -0.01, org inexistente).
- `test_fx.py`: la conversión a USD de una factura conocida en USD, ARS y EUR.
- `test_idempotencia.py`: re-ejecutar un job no cambia los conteos.
