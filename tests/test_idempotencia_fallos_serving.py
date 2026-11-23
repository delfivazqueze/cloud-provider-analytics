"""Idempotencia del pipeline de punta a punta, manejo de fallos y carga a Cassandra (con sesión simulada)."""
import json
import os

import pytest
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from src.common.run_log import RunLog
from src.jobs import serving
from src.jobs.bronze_batch import ingest_master
from src.jobs.bronze_stream import progress_summary, start_bronze_queries
from src.jobs.gold import build_gold
from src.jobs.silver import build_dim_org, build_silver_events

ORGS_CSV = ("org_id,org_name,industry,hq_region,plan_tier,is_enterprise,signup_date,sales_rep,lifecycle_stage,"
            "marketing_source,nps_score\n"
            "org_a,Org A,Retail,us-east,pro,True,2024-01-10,ana,active,ads,40\n"
            "org_b,Org B,Fintech,eu-west,free,False,2024-03-02,juan,active,web,101\n")


def _evento(eid, ts, org="org_a", cost=1.0, value="10.0", service="compute"):
    return {"event_id": eid, "timestamp": ts, "org_id": org, "resource_id": "res_1", "service": service,
            "region": "us-east", "metric": "requests", "value": value, "unit": "count",
            "cost_usd_increment": cost, "schema_version": 1}


def _armar_landing(cfg):
    land = cfg["paths"]["landing"]
    os.makedirs(f"{land}/usage_events_stream")
    with open(f"{land}/customers_orgs.csv", "w") as f:
        f.write(ORGS_CSV)
    archivos = {
        "part_0.jsonl": [_evento("e1", "2025-08-01T10:00:00Z"), _evento("e2", "2025-08-02T10:00:00Z", cost=-3.0),
                         _evento("e3", "2025-08-02T11:00:00Z", org="org_b", value=None, cost=-1.0)],
        "part_1.jsonl": [_evento("e1", "2025-08-01T10:00:00Z"), _evento("e4", "2025-08-01T12:00:00Z", org="org_b")],
    }
    for nombre, filas in archivos.items():
        with open(f"{land}/usage_events_stream/{nombre}", "w") as f:
            f.write("\n".join(json.dumps(x) for x in filas))


def _correr_batch(spark, cfg, log):
    ingest_master(spark, cfg, "customers_orgs", "test", log)
    dim = build_dim_org(spark, cfg, "test", log)
    silver = build_silver_events(spark, cfg, dim, "test", log)
    mart, _ = build_gold(spark, cfg, "test", log)
    return {"silver": silver.count(),
            "quarantine": spark.read.parquet(f"{cfg['paths']['quarantine']}/usage_events_silver").count(),
            "gold": mart.count(),
            "costo_gold": mart.agg(F.round(F.sum("daily_cost_usd"), 4)).first()[0],
            "claves_dup": mart.groupBy("org_id", "usage_date", "service").count().filter("count > 1").count()}


def test_pipeline_idempotente_de_punta_a_punta(spark, cfg, fake_log):
    _armar_landing(cfg)
    qb, qq = start_bronze_queries(spark, cfg, "test")
    qb.awaitTermination(); qq.awaitTermination()

    primera = _correr_batch(spark, cfg, fake_log)
    # e1 duplicado se descarta; e2 (negativo con consumo) se corrige; e3 (negativo sin consumo) va a quarantine
    assert primera == {"silver": 3, "quarantine": 1, "gold": 3, "costo_gold": 5.0, "claves_dup": 0}

    qb, qq = start_bronze_queries(spark, cfg, "test")
    qb.awaitTermination(); qq.awaitTermination()
    assert progress_summary(qb)["filas_leidas"] == 0          # el checkpoint no relee archivos
    segunda = _correr_batch(spark, cfg, fake_log)
    assert segunda == primera
    assert spark.read.parquet(f"{cfg['paths']['bronze']}/customers_orgs").count() == 2


def test_json_corrupto_va_a_quarantine(spark, cfg):
    src = f"{cfg['paths']['landing']}/usage_events_stream"
    os.makedirs(src)
    with open(f"{src}/part_0.jsonl", "w") as f:
        f.write(json.dumps(_evento("e1", "2025-08-01T10:00:00Z")) + "\n{esto no es json\n")
    qb, qq = start_bronze_queries(spark, cfg, "test")
    qb.awaitTermination(); qq.awaitTermination()
    assert spark.read.parquet(f"{cfg['paths']['bronze']}/usage_events").count() == 1
    q = spark.read.parquet(f"{cfg['paths']['quarantine']}/usage_events_bronze")
    assert [r.rule_id for r in q.collect()] == ["B02_CLAVE_NULA"]


def test_fallo_queda_registrado_en_run_log(spark, cfg):
    os.makedirs(cfg["paths"]["landing"])
    log = RunLog(spark, cfg["paths"]["meta"], "run_falla")
    with pytest.raises(AnalysisException):
        with log.track("bronze_batch", "bronze", "users"):
            ingest_master(spark, cfg, "users", log.run_id, log)     # users.csv no existe en Landing
    fila = log.read().first()
    assert fila.estado == "ERROR" and fila.dataset == "users" and "AnalysisException" in fila.detalle


def test_conciliacion_fallida(spark, cfg):
    log = RunLog(spark, cfg["paths"]["meta"], "run_x")
    assert log.add("job", "silver", "ds", entrada=10, validas=8, quarantine=1) == "CONCILIACION_FALLIDA"
    assert log.add("job", "silver", "ds", entrada=10, validas=8, quarantine=1, duplicados=1) == "OK"


class FakeSession:
    def __init__(self, conteos=None):
        self.prepared, self.conteos = [], conteos or {}

    def prepare(self, cql):
        self.prepared.append(cql)
        return cql

    def execute(self, stmt, params):
        n = self.conteos.get(params[0], 0)
        return type("R", (), {"one": lambda self_: (n,)})()


def test_carga_a_cassandra_por_lotes_y_errores(spark, monkeypatch):
    llamadas = []

    def fake_concurrent(session, stmt, rows, concurrency, raise_on_first_error):
        llamadas.append(list(rows))
        return [(r[0] != "x3", None) for r in rows]             # la fila x3 falla

    monkeypatch.setattr(serving, "execute_concurrent_with_args", fake_concurrent)
    df = spark.createDataFrame([(f"x{i}", float(i)) for i in range(5)], "org_id string, v double")
    s = FakeSession()
    enviadas, errores = serving.load_dataframe(s, df, "ks.t", ["org_id", "v"], chunk=2)
    assert (enviadas, errores) == (5, 1)
    assert [len(c) for c in llamadas] == [2, 2, 1]              # se envía en lotes
    assert s.prepared == ["INSERT INTO ks.t (org_id, v) VALUES (?, ?)"]


def test_conteo_por_particion():
    s = FakeSession(conteos={"org_a": 3, "org_b": 4})
    assert serving.count_rows(s, "ks.t", ["org_a", "org_b"]) == 7
    assert s.prepared == ["SELECT COUNT(*) FROM ks.t WHERE org_id = ?"]
