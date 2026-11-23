"""Bronze -> Silver: dim_org (maestro conformado) y usage_events (eventos conformados y enriquecidos)."""
import time

from pyspark.sql import functions as F

from src.common.schemas import METRIC_UNIT
from src.jobs.quality import event_silver_rules, split_by_rules


def latest_snapshot(spark, path):
    """Los maestros en Bronze son snapshots por ingest_date: Silver usa el último."""
    df = spark.read.parquet(path)
    last = df.agg(F.max("ingest_date")).collect()[0][0]
    return df.filter(F.col("ingest_date") == last)


def build_dim_org(spark, cfg, run_id, run_log):
    """SCD tipo 1 (última versión por org_id). NPS fuera de [-100, 100] -> null + flag."""
    t0 = time.time()
    q = cfg["quality"]
    b = latest_snapshot(spark, f"{cfg['paths']['bronze']}/customers_orgs")
    nps_ok = F.col("nps_score").between(q["nps_min"], q["nps_max"])

    dim = (b.select(
                F.trim("org_id").alias("org_id"),
                F.trim("org_name").alias("org_name"),
                F.trim("industry").alias("industry"),
                F.lower(F.trim("hq_region")).alias("hq_region"),
                F.lower(F.trim("plan_tier")).alias("plan_tier"),
                (F.lower(F.trim("is_enterprise")) == "true").alias("is_enterprise"),
                F.to_date("signup_date").alias("signup_date"),
                F.trim("sales_rep").alias("sales_rep"),
                F.lower(F.trim("lifecycle_stage")).alias("lifecycle_stage"),
                F.lower(F.trim("marketing_source")).alias("marketing_source"),
                F.when(nps_ok, F.col("nps_score")).alias("nps_score"),
                (F.col("nps_score").isNotNull() & ~nps_ok).alias("is_nps_out_of_range"),
                "source_file")
           .withColumn("updated_ts", F.current_timestamp())
           .withColumn("run_id", F.lit(run_id)))

    dim.coalesce(1).write.mode("overwrite").parquet(f"{cfg['paths']['silver']}/dim_org")
    dim = spark.read.parquet(f"{cfg['paths']['silver']}/dim_org")
    n = dim.count()
    run_log.add("silver_dim_org", "silver", "dim_org", b.count(), n, 0, 0, time.time() - t0,
                detalle=f"nps_fuera_de_rango={dim.filter('is_nps_out_of_range').count()}")
    return dim


def _metric_unit_expr():
    m = F.create_map(*[x for k, v in METRIC_UNIT.items() for x in (F.lit(k), F.lit(v))])
    return m[F.col("metric")]


def conform_events(bronze, dim_org):
    """Normaliza, compatibiliza v1/v2, imputa unit, corrige el signo del costo y enriquece con dim_org
    (sin aplicar reglas todavía)."""
    unit_missing = F.col("unit").isNull() & F.col("value_num").isNotNull()
    cost = F.col("cost_usd_raw")
    return (bronze
        .withColumn("service", F.lower(F.trim("service")))
        .withColumn("region", F.lower(F.trim("region")))
        .withColumn("metric", F.lower(F.trim("metric")))
        .withColumn("unit", F.lower(F.trim("unit")))
        # R3: unit faltante con value informado -> se imputa desde metric (relación 1:1 medida en E1, D08)
        .withColumn("unit_imputed", unit_missing & _metric_unit_expr().isNotNull())
        .withColumn("unit", F.when(F.col("unit_imputed"), _metric_unit_expr()).otherwise(F.col("unit")))
        # R2 (D29): cost_usd_increment es el cargo por un consumo medido; los créditos viven en facturación.
        # Un costo negativo con consumo (value > 0) es un error de signo: se corrige y queda marcado.
        # Sin consumo no hay cómo validarlo: sigue negativo y la regla S01 lo manda a quarantine.
        .withColumn("cost_usd_raw", F.col("cost_usd_increment"))
        .withColumn("is_cost_sign_corrected", (cost < 0) & (F.col("value_num") > 0))
        .withColumn("cost_usd_increment", F.when(F.col("is_cost_sign_corrected"), F.abs(cost)).otherwise(cost))
        # v1 no trae carbon_kg ni genai_tokens: quedan null (no 0) para no confundir "no medido" con "cero"
        .withColumn("carbon_kg", F.col("carbon_kg").cast("double"))
        .withColumn("genai_tokens", F.col("genai_tokens").cast("long"))
        # features a nivel evento (se suman en Gold)
        .withColumn("requests", F.when(F.col("metric") == "requests", F.col("value_num")))
        .withColumn("cpu_hours", F.when(F.col("metric") == "cpu_hours", F.col("value_num")))
        .withColumn("storage_gb_hours", F.when(F.col("metric") == "storage_gb_hours", F.col("value_num")))
        .join(F.broadcast(dim_org.select("org_id", "org_name", "industry", "plan_tier", "is_enterprise")),
              "org_id", "left"))


def add_cost_anomaly(df, threshold):
    """Score robusto MAD por (service, metric): 0.6745 * |x - mediana| / MAD (D09).
    Devuelve el DataFrame con mad_score e is_cost_anomaly, y la tabla de parámetros por grupo."""
    c = "cost_usd_increment"
    med = df.groupBy("service", "metric").agg(F.percentile_approx(c, 0.5, 10000).alias("cost_median"))
    dev = df.join(F.broadcast(med), ["service", "metric"]).withColumn("abs_dev", F.abs(F.col(c) - F.col("cost_median")))
    mad = dev.groupBy("service", "metric").agg(F.percentile_approx("abs_dev", 0.5, 10000).alias("cost_mad"))
    params = med.join(mad, ["service", "metric"])

    scored = (df.join(F.broadcast(params), ["service", "metric"])
                .withColumn("mad_score", F.when(F.col("cost_mad") > 0,
                            F.round(0.6745 * F.abs(F.col(c) - F.col("cost_median")) / F.col("cost_mad"), 3)))
                .withColumn("is_cost_anomaly", F.coalesce(F.col("mad_score") > threshold, F.lit(False)))
                .drop("cost_median", "cost_mad"))
    return scored, params


SILVER_COLS = ["event_id", "event_ts", "event_date", "org_id", "org_name", "industry", "plan_tier", "is_enterprise",
               "resource_id", "service", "region", "metric", "value_raw", "value_num", "unit", "unit_imputed",
               "cost_usd_increment", "cost_usd_raw", "is_cost_sign_corrected", "mad_score", "is_cost_anomaly",
               "requests", "cpu_hours", "storage_gb_hours", "genai_tokens", "carbon_kg", "schema_version",
               "ingest_ts", "source_file", "run_id"]


def build_silver_events(spark, cfg, dim_org, run_id, run_log):
    t0 = time.time()
    p, q = cfg["paths"], cfg["quality"]
    bronze = spark.read.parquet(f"{p['bronze']}/usage_events")
    n_in = bronze.count()

    conf = conform_events(bronze.drop("run_id"), dim_org).withColumn("run_id", F.lit(run_id))
    validos, quar = split_by_rules(conf, event_silver_rules(list(METRIC_UNIT)))
    validos = validos.cache()

    scored, params = add_cost_anomaly(validos, q["anomaly_threshold"])
    (scored.select(*SILVER_COLS).repartition("event_date")
        .write.mode("overwrite").partitionBy("event_date").parquet(f"{p['silver']}/usage_events"))

    quar = quar.withColumn("is_cost_anomaly", F.col("rule_id") == "S01_COSTO_NEGATIVO_SIN_CONSUMO")
    quar.coalesce(1).write.mode("overwrite").partitionBy("rule_id").parquet(f"{p['quarantine']}/usage_events_silver")
    (params.withColumn("threshold", F.lit(q["anomaly_threshold"])).withColumn("run_id", F.lit(run_id))
        .coalesce(1).write.mode("overwrite").parquet(f"{p['meta']}/anomaly_params"))

    silver = spark.read.parquet(f"{p['silver']}/usage_events")
    n_ok, n_q = silver.count(), spark.read.parquet(f"{p['quarantine']}/usage_events_silver").count()
    validos.unpersist()
    run_log.add("silver_usage_events", "silver", "usage_events", n_in, n_ok, n_q, 0, time.time() - t0,
                detalle=f"unit_imputada={silver.filter('unit_imputed').count()}; "
                        f"signo_corregido={silver.filter('is_cost_sign_corrected').count()}; "
                        f"anomalias={silver.filter('is_cost_anomaly').count()}")
    return silver


def conform_billing(bronze, dim_org, currencies):
    """Facturación conformada. revenue_usd = (subtotal − credits + taxes) × fx (D12).
    En facturas en USD el tipo de cambio debería ser 1: si viene distinto se usa 1 y queda el flag (D23)."""
    cur = F.upper(F.trim("currency"))
    fx_in = F.col("exchange_rate_to_usd")
    fx = F.when(cur == "USD", F.lit(1.0)).otherwise(fx_in)
    credits = F.coalesce(F.col("credits"), F.lit(0.0))
    return (bronze
        .withColumn("currency", cur)
        .withColumn("billing_month_date", F.to_date("month"))
        .withColumn("billing_month", F.date_format("billing_month_date", "yyyy-MM"))
        .withColumn("credits", credits)
        .withColumn("fx_applied", fx)
        .withColumn("is_fx_overridden", (cur == "USD") & (F.abs(fx_in - 1.0) > 1e-9))
        .withColumn("is_adjustment", F.col("subtotal") < 0)
        .withColumn("subtotal_usd", F.round(F.col("subtotal") * fx, 4))
        .withColumn("credits_usd", F.round(credits * fx, 4))
        .withColumn("taxes_usd", F.round(F.col("taxes") * fx, 4))
        .withColumn("revenue_usd", F.round((F.col("subtotal") - credits + F.col("taxes")) * fx, 4))
        .join(F.broadcast(dim_org.select("org_id", "org_name")), "org_id", "left"))


BILLING_COLS = ["invoice_id", "org_id", "org_name", "billing_month", "billing_month_date", "currency",
                "subtotal", "credits", "taxes", "exchange_rate_to_usd", "fx_applied", "is_fx_overridden",
                "is_adjustment", "subtotal_usd", "credits_usd", "taxes_usd", "revenue_usd",
                "source_file", "ingest_ts", "run_id"]


def build_silver_billing(spark, cfg, dim_org, run_id, run_log):
    from src.jobs.quality import billing_silver_rules
    t0 = time.time()
    p = cfg["paths"]
    bronze = spark.read.parquet(f"{p['bronze']}/billing_monthly")
    raw_credits_null = F.col("credits").isNull()
    n_in = bronze.count()
    conf = (conform_billing(bronze.withColumn("_credits_null", raw_credits_null).drop("run_id"),
                            dim_org, cfg["quality"]["currencies"])
            .withColumn("is_credits_missing", F.col("_credits_null")).drop("_credits_null")
            .withColumn("run_id", F.lit(run_id)))
    validos, quar = split_by_rules(conf, billing_silver_rules(cfg["quality"]["currencies"]))
    (validos.select(*BILLING_COLS, "is_credits_missing").coalesce(1)
        .write.mode("overwrite").partitionBy("billing_month").parquet(f"{p['silver']}/billing_usd"))
    quar.coalesce(1).write.mode("overwrite").partitionBy("rule_id").parquet(f"{p['quarantine']}/billing_silver")
    silver = spark.read.parquet(f"{p['silver']}/billing_usd")
    n_ok = silver.count()
    run_log.add("silver_billing_usd", "silver", "billing_usd", n_in, n_ok, n_in - n_ok, 0, time.time() - t0,
                detalle=f"fx_usd_forzado_a_1={silver.filter('is_fx_overridden').count()}; "
                        f"ajustes={silver.filter('is_adjustment').count()}")
    return silver


def conform_tickets(bronze, dim_org, csat_min, csat_max):
    """Tickets conformados. CSAT fuera de [1, 5] -> null + flag (D24)."""
    csat_ok = F.col("csat").between(csat_min, csat_max)
    created = F.to_date("created_at")
    resolved = F.to_date("resolved_at")
    return (bronze
        .withColumn("severity", F.lower(F.trim("severity")))
        .withColumn("category", F.lower(F.trim("category")))
        .withColumn("created_date", created)
        .withColumn("resolved_date", resolved)
        .withColumn("is_open", resolved.isNull())
        .withColumn("resolution_days", F.datediff(resolved, created))
        .withColumn("sla_breached", F.lower(F.trim("sla_breached")) == "true")
        .withColumn("is_critical", F.col("severity") == "critical")
        .withColumn("is_csat_out_of_range", F.col("csat").isNotNull() & ~csat_ok)
        .withColumn("csat", F.when(csat_ok, F.col("csat")))
        .join(F.broadcast(dim_org.select("org_id", "org_name")), "org_id", "left"))


TICKET_COLS = ["ticket_id", "org_id", "org_name", "category", "severity", "is_critical", "created_date",
               "resolved_date", "is_open", "resolution_days", "sla_breached", "csat", "is_csat_out_of_range",
               "source_file", "ingest_ts", "run_id"]


def build_silver_tickets(spark, cfg, dim_org, run_id, run_log):
    from src.jobs.quality import tickets_silver_rules
    t0 = time.time()
    p, q = cfg["paths"], cfg["quality"]
    bronze = latest_snapshot(spark, f"{p['bronze']}/support_tickets")
    n_in = bronze.count()
    conf = conform_tickets(bronze.drop("run_id"), dim_org, q["csat_min"], q["csat_max"]).withColumn("run_id", F.lit(run_id))
    validos, quar = split_by_rules(conf, tickets_silver_rules())
    validos.select(*TICKET_COLS).coalesce(1).write.mode("overwrite").parquet(f"{p['silver']}/support_tickets")
    quar.coalesce(1).write.mode("overwrite").partitionBy("rule_id").parquet(f"{p['quarantine']}/tickets_silver")
    silver = spark.read.parquet(f"{p['silver']}/support_tickets")
    n_ok = silver.count()
    run_log.add("silver_support_tickets", "silver", "support_tickets", n_in, n_ok, n_in - n_ok, 0, time.time() - t0,
                detalle=f"csat_fuera_de_rango={silver.filter('is_csat_out_of_range').count()}; "
                        f"abiertos={silver.filter('is_open').count()}")
    return silver
