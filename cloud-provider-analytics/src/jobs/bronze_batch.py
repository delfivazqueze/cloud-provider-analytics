"""Landing -> Bronze (batch) para los maestros y la facturación."""
import time

from pyspark.sql import functions as F, types as T

from src.common.schemas import CSV_SCHEMAS, NATURAL_KEYS
from src.jobs.quality import split_by_rules

CORRUPT = "_corrupt_record"


def read_csv(spark, landing, name):
    # se agrega la columna de registro corrupto para no perder filas mal formadas (modo PERMISSIVE)
    schema = T.StructType(CSV_SCHEMAS[name].fields + [T.StructField(CORRUPT, T.StringType(), True)])
    return (spark.read.option("header", True)
            .option("escape", '"')          # tags_json trae comillas escapadas como "" (estándar CSV)
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT)
            .schema(schema)
            .csv(f"{landing}/{name}.csv"))


def ingest_master(spark, cfg, name, run_id, run_log):
    """Lee un CSV con esquema explícito, separa inválidos, deduplica y escribe Bronze."""
    t0 = time.time()
    paths = cfg["paths"]
    keys = NATURAL_KEYS[name]

    # source_file se calcula antes de cualquier shuffle (después del dedupe input_file_name queda vacío)
    raw = (read_csv(spark, paths["landing"], name)
           .withColumn("source_file", F.input_file_name())
           .cache())   # cache: Spark no deja filtrar por _corrupt_record sobre el CSV sin cachear
    n_in = raw.count()

    key_null = F.lit(False)
    for k in keys:
        key_null = key_null | F.col(k).isNull()
    validos, quar = split_by_rules(raw, [
        ("B01_FILA_CORRUPTA", F.col(CORRUPT).isNotNull()),
        ("B02_CLAVE_NULA", key_null),
    ])

    tech = lambda df: (df.withColumn("ingest_ts", F.current_timestamp())
                         .withColumn("ingest_date", F.current_date())
                         .withColumn("run_id", F.lit(run_id)))

    bronze = tech(validos.drop(CORRUPT).dropDuplicates(keys))
    if name == "billing_monthly":
        bronze = bronze.withColumn("billing_month", F.date_format(F.to_date("month"), "yyyy-MM"))
        part = "billing_month"
    else:
        part = "ingest_date"

    bronze.coalesce(1).write.mode("overwrite").partitionBy(part).parquet(f"{paths['bronze']}/{name}")
    n_ok = bronze.count()

    n_q = quar.count()
    if n_q:
        tech(quar).write.mode("overwrite").partitionBy("rule_id").parquet(f"{paths['quarantine']}/{name}")
    n_dup = n_in - n_q - n_ok
    raw.unpersist()
    run_log.add("bronze_batch", "bronze", name, n_in, n_ok, n_q, n_dup, time.time() - t0,
                detalle=f"particion={part}")
    return n_in, n_ok, n_q
