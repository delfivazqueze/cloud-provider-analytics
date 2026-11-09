"""Silver -> Gold: mart FinOps org_daily_usage_by_service y vista top_services_14d para serving."""
import time

from pyspark.sql import Window, functions as F

MART_COLS = ["org_id", "usage_date", "service", "org_name", "daily_cost_usd", "requests", "cpu_hours",
             "storage_gb_hours", "genai_tokens", "carbon_kg", "events", "anomaly_events"]


def org_daily_usage_by_service(silver):
    """Grano: (org_id, usage_date, service). Es la misma agregación que el MapReduce de la E1, ahora desde Silver."""
    return (silver.groupBy("org_id", F.col("event_date").alias("usage_date"), "service")
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.round(F.sum("cost_usd_increment"), 4).alias("daily_cost_usd"),
                 F.sum("requests").alias("requests"),
                 F.round(F.sum("cpu_hours"), 4).alias("cpu_hours"),
                 F.round(F.sum("storage_gb_hours"), 4).alias("storage_gb_hours"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.round(F.sum("carbon_kg"), 4).alias("carbon_kg"),
                 F.count("*").alias("events"),
                 F.sum(F.col("is_cost_anomaly").cast("int")).alias("anomaly_events"))
            .select(*MART_COLS))


def top_services_14d(mart, window_days=14):
    """Para cada (org, as_of_date): ranking de servicios por costo de los últimos 14 días.
    Cassandra no ordena por un agregado, así que el Top-N se precalcula acá (D13)."""
    bounds = mart.agg(F.min("usage_date").alias("d0"), F.max("usage_date").alias("d1")).collect()[0]
    # solo fechas con 14 días completos de historia
    as_of = (mart.select(F.col("usage_date").alias("as_of_date")).distinct()
             .filter(F.col("as_of_date") >= F.date_add(F.lit(bounds["d0"]), window_days - 1)))
    win = (mart.join(F.broadcast(as_of),
                     (F.col("usage_date") <= F.col("as_of_date")) &
                     (F.col("usage_date") > F.date_sub(F.col("as_of_date"), window_days)))
           .groupBy("org_id", "as_of_date", "service")
           .agg(F.round(F.sum("daily_cost_usd"), 4).alias("cost_14d_usd"),
                F.sum("requests").alias("requests_14d")))
    w = Window.partitionBy("org_id", "as_of_date").orderBy(F.desc("cost_14d_usd"), "service")
    return win.withColumn("rank", F.row_number().over(w)).select(
        "org_id", "as_of_date", "rank", "service", "cost_14d_usd", "requests_14d")


def build_gold(spark, cfg, run_id, run_log):
    t0 = time.time()
    p = cfg["paths"]
    silver = spark.read.parquet(f"{p['silver']}/usage_events")
    mart = org_daily_usage_by_service(silver).withColumn("run_id", F.lit(run_id))
    (mart.repartition("usage_date").write.mode("overwrite").partitionBy("usage_date")
         .parquet(f"{p['gold']}/org_daily_usage_by_service"))
    mart = spark.read.parquet(f"{p['gold']}/org_daily_usage_by_service")

    top = top_services_14d(mart).withColumn("run_id", F.lit(run_id))
    top.coalesce(1).write.mode("overwrite").parquet(f"{p['gold']}/top_services_14d")
    top = spark.read.parquet(f"{p['gold']}/top_services_14d")

    n_mart = mart.count()
    run_log.add("gold_org_daily_usage", "gold", "org_daily_usage_by_service", silver.count(), n_mart,
                duracion_s=time.time() - t0, conciliar=False,
                detalle="entrada=eventos Silver; salida=filas (org, fecha, servicio)")
    run_log.add("gold_top_services_14d", "gold", "top_services_14d", n_mart, top.count(), conciliar=False,
                detalle="vista derivada del mart para la consulta Q2")
    return mart, top
