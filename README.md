# Cloud Provider Analytics

Pipeline de datos **ETL + Streaming + Serving** para un proveedor de nube: ingesta, limpieza, conformado y publicación de datos de clientes para **FinOps**, **Soporte** y **Producto**.

**Big Data · ITBA · 2C 2026** — Prof. Diego Mosquera
**Grupo:** Cayetana Peces · Abril Solé · Agustina Edalian · Delfina Vázquez Escati

| Entrega | Estado | Documento |
|---|---|---|
| 1ª · Diseño y fundación de datos | ✅ entregada | [`docs/DISENO.md`](docs/DISENO.md) |
| 2ª · Implementación técnica | ✅ entregada | [`docs/ENTREGA2.md`](docs/ENTREGA2.md) |
| Final · MVP integrado | ✅ entregada | [`docs/DOCUMENTACION_TECNICA.md`](docs/DOCUMENTACION_TECNICA.md) |

---

## Arquitectura final

Landing → Bronze (batch para los CSV, Structured Streaming para los eventos) → Silver → Gold (Parquet) → **Cassandra/AstraDB** → FinOps, Soporte y Producto. Patrón **Lambda** (D01): el streaming llega hasta Bronze y Silver/Gold se recalculan en batch desde Bronze (D16, D26).

![Arquitectura final](docs/img/arquitectura_final.png)

| Dominio | Mart Gold | Grano | Consulta en AstraDB |
|---|---|---|---|
| FinOps | `org_daily_usage_by_service` | org_id, usage_date, service | Q1 costos y requests diarios en un rango |
| FinOps | `top_services_14d` (vista del anterior) | org_id, as_of_date, rank | Q2 top-N servicios de los últimos 14 días |
| FinOps | `revenue_by_org_month` | org_id, billing_month | Q4 revenue mensual en USD |
| FinOps | `cost_anomaly_mart` | org_id, usage_date, service | — (consumo analítico en Gold) |
| Soporte | `tickets_by_org_date` / `tickets_by_severity_date` | org_id, ticket_date, severity / severity, ticket_date | Q3 críticos y SLA breach por día |
| Producto / GenAI | `genai_tokens_by_org_date` | org_id, usage_date | Q5 tokens y costo estimado por día |

## Estructura del repositorio

```text
.
├── README.md                 ← este archivo (Quickstart)
├── DECISIONS.md              ← decisiones técnicas y trade-offs (D01–D30)
├── config/pipeline.example.yaml   ← configuración externalizada (sin credenciales)
├── src/
│   ├── common/               ← esquemas, lectura de config, run_log
│   └── jobs/                 ← bronze_batch, bronze_stream, quality, silver, gold, serving
├── notebooks/
│   ├── 01_exploracion_y_bronze.ipynb   ← 1ª entrega: perfil de datos + MapReduce
│   ├── 02_pipeline_e2.ipynb            ← 2ª entrega: pipeline mínimo (salidas con el código de esa entrega)
│   └── 03_mvp_final.ipynb              ← entrega final: pipeline completo end-to-end
├── cql/                      ← 01_schema.cql (5 tablas) · 02_consultas.cql (Q1–Q5)
├── tests/                    ← pytest con SparkSession local (21 tests)
├── docs/                     ← documentación técnica, diseño, gobierno, diccionario, diagramas
├── evidence/                 ← salidas de ejecución (run_log, quarantine, tamaños, tests, consultas, capturas)
├── data/README.md            ← cómo obtener el dataset
└── infra/README.md           ← entorno
```

## Quickstart (Google Colab)

**Requisitos:** cuenta de Google, el dataset `cloud_provider_challenge_dataset_v1.zip` (campus) y, para el serving, una base de AstraDB con el keyspace `cloud_analytics`, su *Secure Connect Bundle* y un token de aplicación.

1. **Abrir el notebook:** en [Colab](https://colab.research.google.com) → *Archivo → Abrir notebook → GitHub* → `delfivazqueze/cloud-provider-analytics` → `notebooks/03_mvp_final.ipynb`.
2. **Subir los archivos:** panel izquierdo, ícono de **carpeta** → subir a `/content`:
   - `cloud_provider_challenge_dataset_v1.zip`
   - `secure-connect-big-data.zip` (bundle de AstraDB; sin él, el notebook saltea la sección 8).
3. **Cargar el token:** panel izquierdo, ícono de **llave (Secrets)** → *Agregar secreto* → nombre `ASTRA_DB_APPLICATION_TOKEN`, valor el token `AstraCS:...` → activar *Acceso al notebook*.
4. **Ejecutar:** *Entorno de ejecución → Ejecutar todo* (≈6 minutos). La primera celda instala PySpark 3.5.3 y clona el repo para importar `src/`.
5. **Resultado esperado:**
   - Bronze: 7 maestros + `usage_events` (43.200 eventos, 4 micro-lotes, 0 descartados por watermark).
   - Silver: 43.196 eventos (212 con el signo del costo corregido, D29) + 4 en quarantine; `dim_org` (80), `billing_usd` (240) y `support_tickets` (1.000).
   - Gold: `org_daily_usage_by_service` 11.050 · `top_services_14d` 12.314 · `revenue_by_org_month` 240 · `tickets_by_org_date` 984 · `tickets_by_severity_date` 575 · `genai_tokens_by_org_date` 1.131 · `cost_anomaly_mart` 11.050, todos con grano único.
   - Idempotencia: todos los conteos iguales al re-ejecutar; el stream re-lanzado lee 0 filas.
   - AstraDB: 5 tablas cargadas sin errores, resultados de Q1–Q5 y conteo igual al recargar.
   - Evidencias en `/content/evidence/`.

**Pruebas:** en una celda nueva del mismo Colab:
```bash
!cd /content/cloud-provider-analytics && python -m pytest -q tests
```

**Limpieza / reinicio:** última celda del notebook (borra todo el lake menos Landing, incluidos los checkpoints) o *Entorno de ejecución → Desconectar y borrar el entorno*. Para borrar las tablas de Cassandra: `DROP TABLE cloud_analytics.<tabla>;` en la CQL Console.

**Versiones:** Python 3.10+ · PySpark 3.5.3 · Java 11/17 (el de Colab) · cassandra-driver 3.29+ · PyYAML.

### Solución de problemas

| Síntoma | Causa | Solución |
|---|---|---|
| `Credenciales de AstraDB disponibles: False` | falta el bundle en `/content` o el secreto no tiene acceso al notebook | revisar los pasos 2 y 3 |
| Timeout o `NoHostAvailable` al conectar | la base de AstraDB se hiberna si no se usa | en el portal de Astra, entrar a la base y tocar **Resume**; esperar a que diga *Active* |
| `ModuleNotFoundError: src...` | quedó en `/content` una copia vieja del repo | ejecutar `!rm -rf /content/cloud-provider-analytics`, *Reiniciar sesión* y *Ejecutar todo* |
| `Unable to infer schema for Parquet` | se borró una zona a mano pero no su checkpoint | correr la celda de limpieza completa (borra también `_checkpoints`) |

## Configuración

`config/pipeline.example.yaml` concentra rutas, particiones de shuffle, parámetros del stream (`max_files_per_trigger`, `watermark`, `trigger`), umbrales de calidad (tolerancia de costo, umbral de anomalías, rangos de NPS y CSAT, monedas) y el keyspace. Para cambiar valores se copia a `config/pipeline.yaml` (ignorado por git) y tiene prioridad sobre el ejemplo.

## Convenciones

- **Zonas:** `landing/` (inmutable) · `bronze/` · `silver/` · `gold/` · `quarantine/` · `_checkpoints/` · `_meta/`.
- **Nombres:** `snake_case`; particiones Hive `columna=valor`; sufijos `_usd`, `_kg`, `_ts`, `_date`; flags con prefijo `is_`.
- **Columnas técnicas:** `ingest_ts`, `source_file`, `run_id` en todas las zonas.
- **Reglas de calidad:** `rule_id` con prefijo de zona (`B` Bronze, `S` Silver); catálogo en `docs/GOBIERNO.md`.
- **Secretos:** nunca en el repo. Token en *Secrets* de Colab; el bundle está en `.gitignore`.

## Limitaciones

- Los eventos llegan desordenados (cada archivo abarca 60 días), así que no hay una vista incremental de la capa speed hasta Cassandra: la frescura es la de una corrida completa del pipeline (D26).
- El componente analítico es la detección de anomalías por MAD (D20); sin etiquetas no se pueden medir precisión ni recall.
- El "costo estimado" de GenAI es el costo de los eventos del servicio, porque el dataset no trae una tarifa por token (D27).
- La carga a Cassandra usa el driver de Python desde el driver de Spark: alcanza para el volumen del caso; a escala real correspondería el Spark Cassandra Connector (D18).
