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


# ---------------------------------------------------------------------------------------------
# Marts de la entrega final
# ---------------------------------------------------------------------------------------------

def revenue_by_org_month(billing):
    """Grano: (org_id, billing_month). Revenue en USD con créditos e impuestos (Q4)."""
    return (billing.groupBy("org_id", "billing_month")
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.count("*").alias("invoices"),
                 F.concat_ws(",", F.sort_array(F.collect_set("currency"))).alias("currencies"),
                 F.round(F.sum("subtotal_usd"), 2).alias("subtotal_usd"),
                 F.round(F.sum("credits_usd"), 2).alias("credits_usd"),
                 F.round(F.sum("taxes_usd"), 2).alias("taxes_usd"),
                 F.round(F.sum("revenue_usd"), 2).alias("revenue_usd"),
                 F.round(F.avg("fx_applied"), 6).alias("fx_applied"),
                 F.sum(F.col("is_adjustment").cast("int")).alias("adjustments"),
                 F.sum(F.col("is_fx_overridden").cast("int")).alias("fx_overridden")))


def tickets_by_org_date(tickets):
    """Grano: (org_id, ticket_date, severity). Conteos, tasa de SLA breach y CSAT promedio."""
    return (tickets.groupBy("org_id", F.col("created_date").alias("ticket_date"), "severity")
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.count("*").alias("tickets"),
                 F.sum(F.col("sla_breached").cast("int")).alias("sla_breached"),
                 F.round(F.avg(F.col("sla_breached").cast("double")), 4).alias("sla_breach_rate"),
                 F.round(F.avg("csat"), 2).alias("csat_avg"),
                 F.count("csat").alias("csat_responses"),
                 F.sum(F.col("is_open").cast("int")).alias("open_tickets")))


def tickets_by_severity_date(tickets):
    """Vista para Q3: por severidad (más 'all') y día, con todos los días del rango aunque no haya tickets."""
    base = tickets.select(F.col("created_date").alias("ticket_date"), "severity", "sla_breached", "csat")
    both = base.union(base.withColumn("severity", F.lit("all")))
    agg = (both.groupBy("severity", "ticket_date")
           .agg(F.count("*").alias("tickets"),
                F.sum(F.col("sla_breached").cast("int")).alias("sla_breached"),
                F.round(F.avg("csat"), 2).alias("csat_avg")))
    b = tickets.agg(F.min("created_date").alias("d0"), F.max("created_date").alias("d1")).collect()[0]
    dias = tickets.sparkSession.sql(
        f"SELECT explode(sequence(DATE'{b['d0']}', DATE'{b['d1']}', INTERVAL 1 DAY)) AS ticket_date")
    sevs = both.select("severity").distinct()
    return (sevs.crossJoin(dias).join(agg, ["severity", "ticket_date"], "left")
            .fillna(0, ["tickets", "sla_breached"])
            .withColumn("sla_breach_rate",
                        F.when(F.col("tickets") > 0, F.round(F.col("sla_breached") / F.col("tickets"), 4))))


def genai_tokens_by_org_date(silver_events):
    """Grano: (org_id, usage_date). Tokens GenAI y costo de los eventos del servicio genai (Q5).
    Los tokens existen desde schema v2 (18/07); antes quedan null."""
    g = silver_events.filter(F.col("service") == "genai")
    return (g.groupBy("org_id", F.col("event_date").alias("usage_date"))
            .agg(F.first("org_name", ignorenulls=True).alias("org_name"),
                 F.sum("genai_tokens").alias("genai_tokens"),
                 F.round(F.sum("cost_usd_increment"), 4).alias("genai_cost_usd"),
                 F.sum("requests").alias("requests"),
                 F.count("*").alias("events"))
            .withColumn("cost_per_1k_tokens_usd",
                        F.when(F.col("genai_tokens") > 0,
                               F.round(1000 * F.col("genai_cost_usd") / F.col("genai_tokens"), 4))))


def cost_anomaly_mart(silver_events, daily_mart):
    """Grano: (org_id, usage_date, service). Flag/score de anomalía con el contexto del costo:
    cuánto del costo del día viene de eventos anómalos y cómo se compara con el día típico de esa org y servicio."""
    ev = (silver_events.groupBy("org_id", F.col("event_date").alias("usage_date"), "service")
          .agg(F.round(F.sum(F.when(F.col("is_cost_anomaly"), F.col("cost_usd_increment")).otherwise(0.0)), 4)
               .alias("anomalous_cost_usd"),
               F.max("mad_score").alias("max_mad_score"),
               F.sum(F.col("is_cost_sign_corrected").cast("int")).alias("sign_corrected_events")))
    w = Window.partitionBy("org_id", "service")
    return (daily_mart.select("org_id", "usage_date", "service", "org_name", "daily_cost_usd", "events", "anomaly_events")
            .join(ev, ["org_id", "usage_date", "service"])
            .withColumn("median_daily_cost_usd", F.round(F.percentile_approx("daily_cost_usd", 0.5).over(w), 4))
            .withColumn("cost_vs_median", F.when(F.col("median_daily_cost_usd") > 0,
                        F.round(F.col("daily_cost_usd") / F.col("median_daily_cost_usd"), 2)))
            .withColumn("is_anomalous_day", F.col("anomaly_events") > 0))


def build_gold_final(spark, cfg, run_id, run_log):
    """Construye los 4 marts que se suman en la entrega final (el de uso diario lo arma build_gold)."""
    t0 = time.time()
    p = cfg["paths"]
    silver_ev = spark.read.parquet(f"{p['silver']}/usage_events")
    billing = spark.read.parquet(f"{p['silver']}/billing_usd")
    tickets = spark.read.parquet(f"{p['silver']}/support_tickets")
    daily = spark.read.parquet(f"{p['gold']}/org_daily_usage_by_service")

    salidas = {
        "revenue_by_org_month": (revenue_by_org_month(billing), "billing_month"),
        "tickets_by_org_date": (tickets_by_org_date(tickets), None),
        "tickets_by_severity_date": (tickets_by_severity_date(tickets), None),
        "genai_tokens_by_org_date": (genai_tokens_by_org_date(silver_ev), None),
        "cost_anomaly_mart": (cost_anomaly_mart(silver_ev, daily), "usage_date"),
    }
    out = {}
    for name, (df, part) in salidas.items():
        w = df.withColumn("run_id", F.lit(run_id))
        if part:
            w.repartition(part).write.mode("overwrite").partitionBy(part).parquet(f"{p['gold']}/{name}")
        else:
            w.coalesce(1).write.mode("overwrite").parquet(f"{p['gold']}/{name}")
        out[name] = spark.read.parquet(f"{p['gold']}/{name}")
        run_log.add(f"gold_{name}", "gold", name, None, out[name].count(), conciliar=False,
                    duracion_s=time.time() - t0)
    return out
