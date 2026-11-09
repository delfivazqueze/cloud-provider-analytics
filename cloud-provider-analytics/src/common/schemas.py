"""Esquemas explícitos de las fuentes de Landing.

Fuente única de verdad para los jobs batch y streaming (ver DECISIONS.md D06/D07).
- En los CSV, booleanos y fechas se leen como string y se tipan en Silver con reglas explícitas.
- En eventos, `value` se lee como string (llega como número o como texto) y se castea después.
- `carbon_kg` y `genai_tokens` son opcionales: solo existen en schema_version = 2.
"""
from pyspark.sql import types as T

S, D, I, L = T.StringType(), T.DoubleType(), T.IntegerType(), T.LongType()


def _schema(*cols):
    return T.StructType([T.StructField(name, dtype, True) for name, dtype in cols])


CSV_SCHEMAS = {
    "customers_orgs": _schema(("org_id", S), ("org_name", S), ("industry", S), ("hq_region", S), ("plan_tier", S),
                              ("is_enterprise", S), ("signup_date", S), ("sales_rep", S), ("lifecycle_stage", S),
                              ("marketing_source", S), ("nps_score", D)),
    "users": _schema(("user_id", S), ("org_id", S), ("email", S), ("role", S), ("active", S),
                     ("created_at", S), ("last_login", S)),
    "resources": _schema(("resource_id", S), ("org_id", S), ("service", S), ("region", S), ("created_at", S),
                         ("state", S), ("tags_json", S)),
    "support_tickets": _schema(("ticket_id", S), ("org_id", S), ("category", S), ("severity", S), ("created_at", S),
                               ("resolved_at", S), ("csat", D), ("sla_breached", S)),
    "marketing_touches": _schema(("touch_id", S), ("org_id", S), ("campaign", S), ("channel", S), ("timestamp", S),
                                 ("clicked", S), ("converted", S)),
    "nps_surveys": _schema(("org_id", S), ("survey_date", S), ("nps_score", D), ("comment", S)),
    "billing_monthly": _schema(("invoice_id", S), ("org_id", S), ("month", S), ("subtotal", D), ("credits", D),
                               ("taxes", D), ("currency", S), ("exchange_rate_to_usd", D)),
}

EVENTS_SCHEMA = _schema(("event_id", S), ("timestamp", S), ("org_id", S), ("resource_id", S), ("service", S),
                        ("region", S), ("metric", S), ("value", S), ("unit", S), ("cost_usd_increment", D),
                        ("schema_version", I), ("carbon_kg", D), ("genai_tokens", L))

# Claves naturales para deduplicación e idempotencia
NATURAL_KEYS = {
    "customers_orgs": ["org_id"], "users": ["user_id"], "resources": ["resource_id"],
    "support_tickets": ["ticket_id"], "marketing_touches": ["touch_id"], "billing_monthly": ["invoice_id"],
    "nps_surveys": ["org_id", "survey_date"], "usage_events": ["event_id"],
}

# Relación métrica → unidad observada en el perfil (1:1), usada para imputar `unit` faltante (D08)
METRIC_UNIT = {"requests": "count", "cpu_hours": "hours", "storage_gb_hours": "gb_hours"}
