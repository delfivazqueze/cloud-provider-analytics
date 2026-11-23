"""Cada regla manda a quarantine los casos inválidos conocidos y deja pasar los válidos."""
import datetime as dt

from pyspark.sql import functions as F

from src.common.schemas import METRIC_UNIT
from src.jobs.quality import event_bronze_rules, event_silver_rules, split_by_rules
from src.jobs.silver import conform_events

TS = dt.datetime(2025, 8, 1, 10, 0)


def _bronze_rows(spark):
    cols = "event_id string, event_ts timestamp, schema_version int"
    return spark.createDataFrame([
        ("evt_ok", TS, 2),
        (None, TS, 2),          # B02
        ("  ", TS, 1),          # B02 (vacío)
        ("evt_ts", None, 1),    # B03
        ("evt_v3", TS, 3),      # B04
    ], cols)


def test_reglas_bronze(spark):
    validos, quar = split_by_rules(_bronze_rows(spark), event_bronze_rules())
    assert [r.event_id for r in validos.collect()] == ["evt_ok"]
    reglas = {r.rule_id for r in quar.collect()}
    assert reglas == {"B02_CLAVE_NULA", "B03_TIMESTAMP_INVALIDO", "B04_SCHEMA_VERSION"}
    assert quar.filter(F.col("motivo").isNull()).count() == 0     # toda fila en quarantine tiene motivo


def test_primera_regla_que_falla_gana(spark):
    df = spark.createDataFrame([(None, None, 9)], "event_id string, event_ts timestamp, schema_version int")
    _, quar = split_by_rules(df, event_bronze_rules())
    assert quar.first().rule_id == "B02_CLAVE_NULA"


def _silver_input(spark):
    cols = ("event_id string, org_id string, service string, region string, metric string, value_num double, "
            "unit string, cost_usd_increment double, carbon_kg double, genai_tokens long")
    return spark.createDataFrame([
        ("ok", "org_a", "Compute ", "US-EAST", "requests", 10.0, "count", 1.5, None, None),
        ("neg", "org_a", "compute", "us-east", "requests", 10.0, "count", -0.5, None, None),         # signo -> 0.5
        ("neg_chico", "org_a", "compute", "us-east", "requests", 10.0, "count", -0.005, None, None), # signo -> 0.005
        ("neg_sin_valor", "org_a", "compute", "us-east", "requests", None, "count", -2.0, None, None),  # S01
        ("sin_unit", "org_a", "storage", "us-east", "storage_gb_hours", 3.0, None, 0.2, 0.01, None),  # imputar
        ("metrica_rara", "org_a", "storage", "us-east", "iops", 3.0, None, 0.2, None, None),      # S02
        ("sin_org", "org_zzz", "compute", "us-east", "cpu_hours", 1.0, "hours", 0.1, None, None),  # S03
    ], cols)


def _dim(spark):
    return spark.createDataFrame([("org_a", "Org A", "Retail", "pro", False)],
                                 "org_id string, org_name string, industry string, plan_tier string, is_enterprise boolean")


def test_reglas_silver(spark):
    conf = conform_events(_silver_input(spark), _dim(spark))
    validos, quar = split_by_rules(conf, event_silver_rules(list(METRIC_UNIT)))
    q = {r.event_id: r.rule_id for r in quar.collect()}
    assert q == {"neg_sin_valor": "S01_COSTO_NEGATIVO_SIN_CONSUMO", "metrica_rara": "S02_UNIT_NO_IMPUTABLE",
                 "sin_org": "S03_ORG_INEXISTENTE"}

    v = {r.event_id: r for r in validos.collect()}
    assert set(v) == {"ok", "neg", "neg_chico", "sin_unit"}
    assert v["sin_unit"].unit == "gb_hours" and v["sin_unit"].unit_imputed
    assert not v["ok"].unit_imputed
    assert v["ok"].service == "compute" and v["ok"].region == "us-east"   # normalización


def test_correccion_de_signo(spark):
    """D29: un costo negativo con consumo medido es un error de signo; se corrige y se conserva el original."""
    v = {r.event_id: r for r in conform_events(_silver_input(spark), _dim(spark)).collect()}
    assert v["neg"].cost_usd_increment == 0.5 and v["neg"].cost_usd_raw == -0.5 and v["neg"].is_cost_sign_corrected
    assert v["neg_chico"].cost_usd_increment == 0.005 and v["neg_chico"].is_cost_sign_corrected
    assert v["ok"].cost_usd_increment == 1.5 and not v["ok"].is_cost_sign_corrected
    assert v["neg_sin_valor"].cost_usd_increment == -2.0 and not v["neg_sin_valor"].is_cost_sign_corrected
