# Infraestructura

- **Cómputo:** Google Colab, Spark `local[*]` (PySpark 3.5.3). La primera celda del notebook 02 instala dependencias y clona el repo.
- **Almacenamiento:** `/content/datalake` (efímero). Con `trigger(availableNow=True)` el pipeline completo se reconstruye desde Landing en unos minutos (D14). Para persistir entre sesiones: `paths.base: /content/drive/MyDrive/cloud-provider-analytics` en `config/pipeline.yaml` y montar Drive.
- **Serving:** AstraDB (free tier), base `big_data`, región `us-east-2`, keyspace `cloud_analytics`. Conexión con *Secure Connect Bundle* + token de aplicación (rol Database Administrator de esa base).
- **Dependencias Python:** `pyspark==3.5.3`, `cassandra-driver`, `pyyaml`; para tests, `pytest`.
- **Limpieza:** última celda del notebook 02 (borra el lake salvo Landing, incluidos los checkpoints). Tablas de Cassandra: `DROP TABLE` desde la CQL Console.
