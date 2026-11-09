"""Compatibilidad v1/v2, features, anomalías MAD y grano del mart."""
import datetime as dt

from pyspark.sql import functions as F

from src.jobs.gold import org_daily_usage_by_service, top_services_14d
from src.jobs.silver import add_cost_anomaly, conform_events


def test_v1_queda_con_null_y_no_con_cero(spark):
    df = spark.createDataFrame([
        ("v1", "org_a", "genai", "us-east", "requests", 5.0, "count", 1.0, None, None),
        ("v2", "org_a", "genai", "us-east", "requests", 5.0, "count", 1.0, 0.02, 1200),
    ], "event_id string, org_id string, service string, region string, metric string, value_num double, "
       "unit string, cost_usd_increment double, carbon_kg double, genai_tokens long")
    dim = spark.createDataFrame([("org_a", "A", "x", "pro", False)],
                                "org_id string, org_name string, industry string, plan_tier string, is_enterprise boolean")
    r = {x.event_id: x for x in conform_events(df, dim, -0.01).collect()}
    assert r["v1"].carbon_kg is None and r["v1"].genai_tokens is None
    assert r["v2"].genai_tokens == 1200
    assert r["v1"].requests == 5.0 and r["v1"].cpu_hours is None


def test_mad_marca_el_spike_de_su_grupo(spark):
    filas = [("compute", "requests", 10.0 + i * 0.1) for i in range(30)] + [("compute", "requests", 100.0)]
    filas += [("networking", "requests", 1.0 + i * 0.01) for i in range(30)] + [("networking", "requests", 5.0)]
    df = spark.createDataFrame(filas, "service string, metric string, cost_usd_increment double")
    scored, params = add_cost_anomaly(df, 3.5)
    marcados = sorted(r.cost_usd_increment for r in scored.filter("is_cost_anomaly").collect())
    # 5 USD es normal para compute pero anómalo para networking: por eso el umbral es por grupo
    assert marcados == [5.0, 100.0]
    assert params.count() == 2


def _silver(spark):
    d1, d2 = dt.date(2025, 8, 1), dt.date(2025, 8, 2)
    return spark.createDataFrame([
        ("org_a", "A", d1, "compute", 1.0, 10.0, None, None, None, None, False),
        ("org_a", "A", d1, "compute", 2.0, None, 0.5, None, None, 0.01, True),
        ("org_a", "A", d1, "genai", 4.0, 3.0, None, None, 500, 0.02, False),
        ("org_a", "A", d2, "compute", 1.0, 1.0, None, None, None, None, False),
    ], "org_id string, org_name string, event_date date, service string, cost_usd_increment double, "
       "requests double, cpu_hours double, storage_gb_hours double, genai_tokens long, carbon_kg double, "
       "is_cost_anomaly boolean")


def test_mart_grano_y_sumas(spark):
    mart = org_daily_usage_by_service(_silver(spark))
    assert mart.count() == mart.select("org_id", "usage_date", "service").distinct().count() == 3
    r = mart.filter((F.col("service") == "compute") & (F.col("usage_date") == dt.date(2025, 8, 1))).first()
    assert (r.daily_cost_usd, r.requests, r.cpu_hours, r.events, r.anomaly_events) == (3.0, 10.0, 0.5, 2, 1)
    assert r.genai_tokens is None             # sin tokens -> null, no 0
    total = mart.agg(F.sum("daily_cost_usd")).first()[0]
    assert total == 8.0                       # conciliación: Σ Silver = Σ Gold


def test_top_services_14d(spark):
    filas = []
    for d in range(14):
        fecha = dt.date(2025, 8, 1) + dt.timedelta(days=d)
        filas += [("org_a", fecha, "compute", 1.0, 1.0), ("org_a", fecha, "genai", 2.0, 1.0)]
    mart = spark.createDataFrame(filas, "org_id string, usage_date date, service string, daily_cost_usd double, requests double")
    top = top_services_14d(mart).collect()
    assert len(top) == 2                      # solo hay una fecha con 14 días de historia
    assert [(r.rank, r.service, r.cost_14d_usd) for r in sorted(top, key=lambda r: r.rank)] == \
           [(1, "genai", 28.0), (2, "compute", 14.0)]
