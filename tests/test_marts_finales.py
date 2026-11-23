"""Facturación en USD, tickets, GenAI y anomalías (marts de la entrega final)."""
import datetime as dt

from pyspark.sql import functions as F

from src.jobs.gold import cost_anomaly_mart, genai_tokens_by_org_date, revenue_by_org_month, tickets_by_severity_date
from src.jobs.silver import conform_billing, conform_tickets


def _dim(spark):
    return spark.createDataFrame([("org_a", "Org A")], "org_id string, org_name string")


def test_conversion_a_usd(spark):
    """Una factura conocida de cada moneda (test_fx previsto desde la E1)."""
    b = spark.createDataFrame([
        ("i_ars", "org_a", "2025-06-01", 1000.0, None, 210.0, "ARS", 0.0015),   # credits null -> 0
        ("i_eur", "org_a", "2025-06-01", 100.0, 10.0, 21.0, "EUR", 1.10),
        ("i_usd", "org_a", "2025-06-01", 100.0, 0.0, 21.0, "usd ", 0.90),       # USD con fx != 1 -> se usa 1
    ], "invoice_id string, org_id string, month string, subtotal double, credits double, taxes double, "
       "currency string, exchange_rate_to_usd double")
    r = {x.invoice_id: x for x in conform_billing(b, _dim(spark), ["USD", "ARS", "EUR"]).collect()}
    assert r["i_ars"].revenue_usd == round((1000 - 0 + 210) * 0.0015, 4)       # 1.815
    assert r["i_eur"].revenue_usd == round((100 - 10 + 21) * 1.10, 4)           # 122.1
    assert r["i_usd"].revenue_usd == 121.0 and r["i_usd"].is_fx_overridden and r["i_usd"].currency == "USD"
    assert not r["i_eur"].is_fx_overridden
    assert r["i_ars"].billing_month == "2025-06"

    mart = revenue_by_org_month(conform_billing(b, _dim(spark), ["USD", "ARS", "EUR"])).first()
    assert mart.invoices == 3 and abs(mart.revenue_usd - (1.815 + 122.1 + 121.0)) < 0.01
    assert mart.currencies == "ARS,EUR,USD"


def test_csat_fuera_de_rango(spark):
    t = spark.createDataFrame([
        ("t1", "org_a", "high", "2025-08-01", None, 7.0, "True"),
        ("t2", "org_a", "critical", "2025-08-01", "2025-08-03", 4.0, "False"),
        ("t3", "org_a", "low", "2025-08-02", "2025-08-02", 0.0, "False"),
    ], "ticket_id string, org_id string, severity string, created_at string, resolved_at string, csat double, "
       "sla_breached string")
    r = {x.ticket_id: x for x in conform_tickets(t.withColumn("category", F.lit("billing")), _dim(spark), 1, 5).collect()}
    assert r["t1"].csat is None and r["t1"].is_csat_out_of_range and r["t1"].is_open and r["t1"].sla_breached
    assert r["t2"].csat == 4.0 and not r["t2"].is_csat_out_of_range and r["t2"].resolution_days == 2
    assert r["t3"].csat is None and r["t3"].is_csat_out_of_range


def test_serie_diaria_sin_huecos(spark):
    t = spark.createDataFrame([
        (dt.date(2025, 8, 1), "critical", True, 3.0),
        (dt.date(2025, 8, 3), "critical", False, None),
        (dt.date(2025, 8, 3), "low", False, 5.0),
    ], "created_date date, severity string, sla_breached boolean, csat double")
    r = {(x.severity, str(x.ticket_date)): x for x in tickets_by_severity_date(t).collect()}
    assert len(r) == 3 * 3                       # (critical, low, all) x 3 días
    assert r[("critical", "2025-08-02")].tickets == 0 and r[("critical", "2025-08-02")].sla_breach_rate is None
    assert r[("critical", "2025-08-01")].sla_breach_rate == 1.0
    assert r[("all", "2025-08-03")].tickets == 2 and r[("all", "2025-08-03")].sla_breach_rate == 0.0


def test_genai_y_anomalias(spark):
    d = dt.date(2025, 8, 1)
    ev = spark.createDataFrame([
        ("org_a", "A", d, "genai", 2.0, 1000, 5.0, False, 0.5, False),
        ("org_a", "A", d, "genai", 8.0, 3000, None, True, 9.0, False),
        ("org_a", "A", d, "compute", 1.0, None, 2.0, False, 0.1, True),
    ], "org_id string, org_name string, event_date date, service string, cost_usd_increment double, "
       "genai_tokens long, requests double, is_cost_anomaly boolean, mad_score double, is_cost_sign_corrected boolean")
    g = genai_tokens_by_org_date(ev).first()
    assert (g.genai_tokens, g.genai_cost_usd, g.cost_per_1k_tokens_usd) == (4000, 10.0, 2.5)

    daily = spark.createDataFrame([("org_a", d, "genai", "A", 10.0, 2, 1), ("org_a", d, "compute", "A", 1.0, 1, 0)],
                                  "org_id string, usage_date date, service string, org_name string, "
                                  "daily_cost_usd double, events long, anomaly_events long")
    a = {x.service: x for x in cost_anomaly_mart(ev, daily).collect()}
    assert a["genai"].anomalous_cost_usd == 8.0 and a["genai"].is_anomalous_day and a["genai"].max_mad_score == 9.0
    assert a["compute"].anomalous_cost_usd == 0.0 and not a["compute"].is_anomalous_day
    assert a["compute"].sign_corrected_events == 1 and a["genai"].sign_corrected_events == 0
