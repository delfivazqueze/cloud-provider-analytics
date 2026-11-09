"""Landing (usage_events_stream/*.jsonl) -> Bronze con Structured Streaming.

Dos queries sobre la misma fuente, como en el laboratorio de la clase de ETL en streaming:
  1) válidos  -> watermark + dropDuplicatesWithinWatermark(event_id) -> bronze/usage_events
  2) inválidos -> quarantine/usage_events_bronze
Cada query tiene su propio checkpoint: re-ejecutar no vuelve a leer archivos ya procesados.
"""
import shutil

from pyspark.sql import functions as F

from src.common.schemas import EVENTS_SCHEMA
from src.jobs.quality import event_bronze_rules, split_by_rules

BRONZE_COLS = ["event_id", "event_ts", "event_date", "org_id", "resource_id", "service", "region", "metric",
               "value_raw", "value_num", "unit", "cost_usd_increment", "schema_version", "carbon_kg",
               "genai_tokens", "ingest_ts", "source_file", "run_id"]


def read_events_stream(spark, cfg, run_id):
    st = cfg["streaming"]
    return (spark.readStream
            .schema(EVENTS_SCHEMA)                                  # en streaming no se puede inferir
            .option("maxFilesPerTrigger", st["max_files_per_trigger"])  # admission control
            .json(f"{cfg['paths']['landing']}/usage_events_stream")
            .withColumn("source_file", F.input_file_name())
            .withColumnRenamed("value", "value_raw")
            .withColumn("event_ts", F.to_timestamp("timestamp"))
            .withColumn("event_date", F.to_date("event_ts"))
            .withColumn("value_num", F.col("value_raw").cast("double"))   # fallback: si no castea queda null
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("run_id", F.lit(run_id)))


def dedupe(df, watermark):
    """Deduplicación por event_id con estado acotado por el watermark."""
    return (df.withWatermark("event_ts", watermark)
              .dropDuplicatesWithinWatermark(["event_id"]))


def _trigger(writer, cfg):
    if cfg["streaming"]["trigger"] == "available_now":
        return writer.trigger(availableNow=True)
    return writer.trigger(processingTime=cfg["streaming"]["processing_time"])


def start_bronze_queries(spark, cfg, run_id, watermark=None):
    p = cfg["paths"]
    watermark = watermark or cfg["streaming"]["watermark"]
    events = read_events_stream(spark, cfg, run_id)
    validos, quar = split_by_rules(events, event_bronze_rules())

    bronze = dedupe(validos, watermark).select(*BRONZE_COLS).repartition("event_date")  # 1 archivo por fecha y lote
    q_bronze = _trigger(bronze.writeStream
                        .queryName("bronze_usage_events")
                        .format("parquet")
                        .option("path", f"{p['bronze']}/usage_events")
                        .option("checkpointLocation", f"{p['checkpoints']}/bronze_usage_events")
                        .partitionBy("event_date")
                        .outputMode("append"), cfg).start()

    q_quar = _trigger(quar.writeStream
                      .queryName("quarantine_usage_events_bronze")
                      .format("parquet")
                      .option("path", f"{p['quarantine']}/usage_events_bronze")
                      .option("checkpointLocation", f"{p['checkpoints']}/quarantine_usage_events_bronze")
                      .partitionBy("rule_id")
                      .outputMode("append"), cfg).start()
    return q_bronze, q_quar


def progress_summary(query):
    """Resume recentProgress: micro-lotes, filas leídas, filas descartadas por watermark y filas emitidas."""
    lotes, entrada, tarde, salida, estado = 0, 0, 0, 0, 0
    for pr in query.recentProgress:
        if pr["numInputRows"] > 0:
            lotes += 1
        entrada += pr["numInputRows"]
        for op in pr.get("stateOperators", []):
            tarde += op.get("numRowsDroppedByWatermark", 0)
            salida += op.get("numRowsUpdated", 0)
            estado = op.get("numRowsTotal", estado)
    return {"micro_lotes": lotes, "filas_leidas": entrada, "descartadas_por_watermark": tarde,
            "emitidas": salida, "claves_en_estado": estado}


def watermark_experiment(spark, cfg, watermark):
    """Corre la misma lectura + dedupe con otro watermark contra un sink 'noop' (no escribe nada).
    Sirve para medir cuántos eventos se perderían por llegar tarde."""
    events = read_events_stream(spark, cfg, "experimento")
    validos, _ = split_by_rules(events, event_bronze_rules())
    safe = watermark.replace(" ", "_")
    ckpt = f"{cfg['paths']['checkpoints']}/_experimento_wm_{safe}"
    shutil.rmtree(ckpt, ignore_errors=True)        # el experimento siempre arranca de cero
    q = (dedupe(validos, watermark).writeStream
         .format("noop")
         .option("checkpointLocation", ckpt)
         .trigger(availableNow=True)
         .start())
    q.awaitTermination()
    return progress_summary(q)
