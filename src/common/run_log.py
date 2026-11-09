"""Registro de ejecuciones en _meta/run_log (observabilidad y conciliación de conteos)."""
import datetime
import uuid

from pyspark.sql import types as T

RUN_LOG_SCHEMA = T.StructType([
    T.StructField("run_id", T.StringType()),
    T.StructField("job", T.StringType()),
    T.StructField("zona", T.StringType()),
    T.StructField("dataset", T.StringType()),
    T.StructField("filas_entrada", T.LongType()),
    T.StructField("filas_validas", T.LongType()),
    T.StructField("filas_quarantine", T.LongType()),
    T.StructField("filas_duplicadas", T.LongType()),
    T.StructField("duracion_s", T.DoubleType()),
    T.StructField("estado", T.StringType()),
    T.StructField("detalle", T.StringType()),
    T.StructField("ts", T.TimestampType()),
])


def new_run_id():
    return datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


class RunLog:
    """Junta las filas del log en memoria y las escribe en Parquet (append) al final de cada job."""

    def __init__(self, spark, meta_path, run_id):
        self.spark = spark
        self.path = f"{meta_path}/run_log"
        self.run_id = run_id
        self.rows = []

    def add(self, job, zona, dataset, entrada, validas, quarantine=0, duplicados=0, duracion_s=0.0,
            estado="OK", detalle="", conciliar=True):
        # conciliación (objetivo O2): entrada = válidas + quarantine + duplicados descartados.
        # En agregaciones (Gold) cambia el grano, así que no se concilia por filas sino por sumas.
        if conciliar and estado == "OK" and entrada is not None and entrada != validas + quarantine + duplicados:
            estado = "CONCILIACION_FALLIDA"
        self.rows.append((self.run_id, job, zona, dataset, entrada, validas, quarantine, duplicados,
                          round(duracion_s, 2), estado, detalle, datetime.datetime.utcnow()))
        return estado

    def flush(self):
        if self.rows:
            df = self.spark.createDataFrame(self.rows, RUN_LOG_SCHEMA)
            df.coalesce(1).write.mode("append").parquet(self.path)
            self.rows = []

    def read(self):
        return self.spark.read.parquet(self.path)
