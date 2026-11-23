"""Reglas de calidad y separación válidos / quarantine.

Cada regla es (rule_id, condición que marca la fila como INVÁLIDA, motivo).
Una fila va a quarantine con la PRIMERA regla que falla; el resto sigue de largo.
La quarantine conserva todas las columnas originales + rule_id + motivo, para poder reprocesar.
"""
from pyspark.sql import functions as F

# Catálogo de reglas (se documenta en docs/GOBIERNO.md)
RULES = {
    "B01_FILA_CORRUPTA": "La fila no respeta el esquema del CSV",
    "B02_CLAVE_NULA": "Clave natural nula (event_id, org_id, invoice_id...)",
    "B03_TIMESTAMP_INVALIDO": "timestamp nulo o no parseable",
    "B04_SCHEMA_VERSION": "schema_version fuera de {1, 2}",
    "S01_COSTO_NEGATIVO_SIN_CONSUMO": "cost_usd_increment < 0 sin consumo medido (value nulo): no se puede validar el signo",
    "S02_UNIT_NO_IMPUTABLE": "value informado sin unit y con metric desconocida",
    "S03_ORG_INEXISTENTE": "org_id no existe en dim_org",
    "S04_MONEDA_INVALIDA": "currency fuera de {USD, ARS, EUR} o exchange_rate_to_usd <= 0",
    "S05_FECHA_INVALIDA": "fecha nula o no parseable",
}


def split_by_rules(df, rules):
    """rules: lista de (rule_id, condicion_invalida). Devuelve (validos, quarantine)."""
    rule_col = F.lit(None).cast("string")
    for rule_id, cond in reversed(rules):          # reversed: gana la primera regla de la lista
        rule_col = F.when(cond, F.lit(rule_id)).otherwise(rule_col)

    motivos = F.create_map(*[x for r in RULES.items() for x in (F.lit(r[0]), F.lit(r[1]))])
    marked = df.withColumn("rule_id", rule_col)
    validos = marked.filter(F.col("rule_id").isNull()).drop("rule_id")
    quarantine = (marked.filter(F.col("rule_id").isNotNull())
                  .withColumn("motivo", motivos[F.col("rule_id")]))
    return validos, quarantine


def event_bronze_rules():
    """Reglas que se aplican en la ingesta de eventos (Bronze)."""
    return [
        ("B02_CLAVE_NULA", F.col("event_id").isNull() | (F.trim("event_id") == "")),
        ("B03_TIMESTAMP_INVALIDO", F.col("event_ts").isNull()),
        ("B04_SCHEMA_VERSION", ~F.coalesce(F.col("schema_version").isin(1, 2), F.lit(False))),
    ]


def event_silver_rules(known_metrics):
    """Reglas de Silver para eventos. Se aplican después de normalizar, imputar unit y corregir el signo (D29):
    a esta altura solo queda un costo negativo si el evento no trae consumo con qué validarlo."""
    return [
        ("S03_ORG_INEXISTENTE", F.col("org_name").isNull()),   # viene del left join con dim_org
        ("S01_COSTO_NEGATIVO_SIN_CONSUMO", F.col("cost_usd_increment") < 0),
        ("S02_UNIT_NO_IMPUTABLE", F.col("value_num").isNotNull() & F.col("unit").isNull()
                                  & ~F.coalesce(F.col("metric").isin(*known_metrics), F.lit(False))),
    ]


def billing_silver_rules(currencies):
    return [
        ("S03_ORG_INEXISTENTE", F.col("org_name").isNull()),
        ("S04_MONEDA_INVALIDA", ~F.coalesce(F.col("currency").isin(*currencies), F.lit(False))
                                | ~F.coalesce(F.col("exchange_rate_to_usd") > 0, F.lit(False))),
        ("S05_FECHA_INVALIDA", F.col("billing_month_date").isNull()),
    ]


def tickets_silver_rules():
    return [
        ("S03_ORG_INEXISTENTE", F.col("org_name").isNull()),
        ("S05_FECHA_INVALIDA", F.col("created_date").isNull()),
    ]
