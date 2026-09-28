# Cloud Provider Analytics

Pipeline de datos **ETL + Streaming + Serving** para un proveedor de nube: ingesta, limpieza, conformado y publicación de datos de clientes para **FinOps**, **Soporte** y **Producto**.

**Big Data · ITBA · 2C 2026** — Prof. Diego Mosquera  
**Grupo:** Cayetana Peces · Abril Solé · Agustina Edalian · Delfina Vázquez Escati

| Entrega | Fecha | Estado |
|---|---|---|
| 1ª · Diseño y fundación de datos | 05/10/2026 (reprogramada) | ✅ entregada: ver [`docs/DISENO.md`](docs/DISENO.md) |
| 2ª · Implementación técnica | 16/11/2026 | ⏳ |
| Final · MVP + defensa | 07/12/2026 | ⏳ |

---

## Arquitectura (v1)

Fuentes → Ingesta (batch + streaming) → Data Lake **Landing / Bronze / Silver / Gold** (Parquet) → Spark → **Cassandra/AstraDB** → Consumo, con calidad, metadatos, linaje, seguridad y observabilidad como capacidades transversales. Patrón **Lambda**: la justificación está en el documento de diseño, §5.

![Arquitectura v1](docs/img/arquitectura_v1.png)

## Estructura del repositorio

```text
.
├── README.md                ← este archivo (Quickstart)
├── DECISIONS.md             ← registro de decisiones técnicas y trade-offs
├── docs/
│   ├── DISENO.md            ← documento de diseño de la 1ª entrega
│   ├── Diseno_Entrega1.pdf  ← mismo documento en PDF
│   ├── plan_correcciones.md ← se completa con el feedback del 05/10
│   └── img/                 ← diagramas (SVG + PNG)
├── notebooks/
│   └── 01_exploracion_y_bronze.ipynb   ← perfil de datos + Landing→Bronze + MapReduce↔Spark
├── src/common/schemas.py    ← esquemas explícitos (fuente única para batch y streaming)
├── config/pipeline.example.yaml        ← configuración externalizada (sin credenciales)
├── data/README.md           ← cómo obtener el dataset
├── evidence/                ← salidas y capturas de ejecución
├── tests/                   ← pruebas (2ª entrega)
└── infra/                   ← scripts de entorno (2ª entrega)
```

## Quickstart (Google Colab)

**Requisitos:** una cuenta de Google. Nada más: Colab trae Python y Java, y el notebook instala PySpark 3.5.3 si hace falta.

1. Abrí [Google Colab](https://colab.research.google.com) → **Archivo → Subir notebook** → elegí `notebooks/01_exploracion_y_bronze.ipynb`.
2. En el panel izquierdo tocá el ícono de **carpeta** y subí `cloud_provider_challenge_dataset_v1.zip` (ver [`data/README.md`](data/README.md)).
3. **Entorno de ejecución → Ejecutar todo** (≈2–3 minutos).
4. Resultado esperado:
   - `/content/datalake/bronze/` con 8 datasets en Parquet (`usage_events` particionado en 60 fechas);
   - `/content/evidence/01_profile_summary.json`;
   - el conteo de idempotencia `43,200 → 43,200`.

**Limpieza / reinicio:** *Entorno de ejecución → Desconectar y borrar el entorno* (o borrar `/content/datalake`). Landing se vuelve a descomprimir desde el zip.

**Versiones:** Python 3.10+ · PySpark 3.5.3 · Java 11/17 (el que trae Colab).

## Datos

Dataset sintético provisto por la cátedra: 7 CSV + 120 archivos JSONL (43.200 eventos, 60 días, evolución de esquema v1 → v2 el 2025-07-18). El perfil completo está en el documento de diseño, §3.

## Convenciones

- **Zonas:** `landing/` (inmutable) · `bronze/` · `silver/` · `gold/` · `quarantine/` · `_checkpoints/` · `_meta/`.
- **Nombres:** `snake_case`; particiones con el formato Hive `columna=valor`; sufijos de unidad `_usd`, `_kg`, `_ts`, `_date`; flags con prefijo `is_`.
- **Columnas técnicas** en todas las tablas: `ingest_ts`, `source_file` (y `run_id` desde la 2ª entrega).
- **Secretos:** nunca en el repo. El token de AstraDB va en los secretos de Colab o en variables de entorno.
- **Commits:** mensajes en español con prefijo (`docs:`, `feat:`, `fix:`, `data:`).

## Limitaciones actuales

La 1ª entrega es de diseño: está implementado Landing → Bronze (batch) más el perfil de datos. Streaming, Silver, Gold, Cassandra y gobierno están diseñados y se implementan en la 2ª entrega (ver el documento de diseño, §12).
