"""Bronze batch (filas corruptas y duplicados) y Bronze streaming (dedupe por event_id + checkpoint)."""
import json
import os

from src.jobs.bronze_batch import ingest_master
from src.jobs.bronze_stream import progress_summary, start_bronze_queries


def test_batch_corruptas_y_duplicados(spark, cfg, fake_log):
    os.makedirs(cfg["paths"]["landing"])
    with open(f"{cfg['paths']['landing']}/billing_monthly.csv", "w") as f:
        f.write("invoice_id,org_id,month,subtotal,credits,taxes,currency,exchange_rate_to_usd\n"
                "inv_1,org_a,2025-06-01,100.0,,21.0,USD,1.0\n"
                "inv_1,org_a,2025-06-01,100.0,,21.0,USD,1.0\n"          # duplicado
                "inv_2,org_a,2025-06-01,no_es_numero,,21.0,ARS,0.001\n"  # corrupta (B01)
                ",org_a,2025-07-01,50.0,,10.0,EUR,1.1\n")               # clave nula (B02)
    n_in, n_ok, n_q = ingest_master(spark, cfg, "billing_monthly", "test", fake_log)
    assert (n_in, n_ok, n_q) == (4, 1, 2)
    q = spark.read.parquet(f"{cfg['paths']['quarantine']}/billing_monthly")
    assert {r.rule_id for r in q.collect()} == {"B01_FILA_CORRUPTA", "B02_CLAVE_NULA"}
    b = spark.read.parquet(f"{cfg['paths']['bronze']}/billing_monthly").first()
    assert b.source_file.endswith("billing_monthly.csv") and b.billing_month == "2025-06"


def _evento(eid, ts, cost=1.0):
    return {"event_id": eid, "timestamp": ts, "org_id": "org_a", "resource_id": "res_1", "service": "compute",
            "region": "us-east", "metric": "requests", "value": "12.0", "unit": "count",
            "cost_usd_increment": cost, "schema_version": 1}


def test_stream_dedupe_y_checkpoint(spark, cfg):
    src = f"{cfg['paths']['landing']}/usage_events_stream"
    os.makedirs(src)
    archivos = {
        "part_0.jsonl": [_evento("e1", "2025-08-30T10:00:00Z"), _evento("e2", "2025-07-05T10:00:00Z")],
        # reentrega del productor: e1 vuelve a llegar + un evento tardío (julio) + uno sin id
        "part_1.jsonl": [_evento("e1", "2025-08-30T10:00:00Z"), _evento("e3", "2025-07-04T08:00:00Z"),
                         _evento(None, "2025-08-01T00:00:00Z")],
    }
    for nombre, filas in archivos.items():
        with open(f"{src}/{nombre}", "w") as f:
            f.write("\n".join(json.dumps(x) for x in filas))

    qb, qq = start_bronze_queries(spark, cfg, "test")
    qb.awaitTermination(); qq.awaitTermination()
    bronze = spark.read.parquet(f"{cfg['paths']['bronze']}/usage_events")
    assert sorted(r.event_id for r in bronze.collect()) == ["e1", "e2", "e3"]   # e1 una sola vez, e3 tardío entra
    assert spark.read.parquet(f"{cfg['paths']['quarantine']}/usage_events_bronze").count() == 1
    assert bronze.filter("value_num = 12.0").count() == 3                        # "12.0" texto -> número

    # re-ejecución: el checkpoint ya tiene los dos archivos, no se lee nada nuevo
    qb, qq = start_bronze_queries(spark, cfg, "test")
    qb.awaitTermination(); qq.awaitTermination()
    assert progress_summary(qb)["filas_leidas"] == 0
    assert spark.read.parquet(f"{cfg['paths']['bronze']}/usage_events").count() == 3
