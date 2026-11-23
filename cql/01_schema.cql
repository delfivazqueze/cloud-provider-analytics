-- Keyspace: cloud_analytics (en AstraDB se crea desde el portal; replicación administrada RF=3)
-- Modelado query-first: una tabla por consulta.

-- Q1. Costos y requests diarios por organización y servicio en un rango de fechas.
--     Partición = org_id (todas las fechas de una org juntas, ~360 filas cada 60 días).
--     Clustering = usage_date DESC, service: el rango de fechas se resuelve dentro de una sola partición.
CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    org_id            text,
    usage_date        date,
    service           text,
    org_name          text,
    daily_cost_usd    double,
    requests          double,
    cpu_hours         double,
    storage_gb_hours  double,
    genai_tokens      bigint,
    carbon_kg         double,
    events            int,
    anomaly_events    int,
    run_id            text,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);

-- Q2. Top-N servicios por costo acumulado en los últimos 14 días para una organización.
--     Cassandra no ordena por un agregado: Spark precalcula el ranking por (org, as_of_date).
--     Partición = (org_id, as_of_date); clustering = rank ASC -> LIMIT N devuelve el Top-N ya ordenado.
CREATE TABLE IF NOT EXISTS top_services_14d (
    org_id         text,
    as_of_date     date,
    rank           int,
    service        text,
    cost_14d_usd   double,
    requests_14d   double,
    run_id         text,
    PRIMARY KEY ((org_id, as_of_date), rank)
) WITH CLUSTERING ORDER BY (rank ASC);

-- Q3. Evolución de tickets críticos y tasa de SLA breach por día (últimos 30 días).
--     Partición = severity (5 particiones: critical, high, medium, low y 'all'); clustering = ticket_date DESC.
--     Una partición tiene una fila por día (~115 en el dataset): 30 días = las primeras 30 filas.
--     Se incluyen los días sin tickets (tickets = 0) para que la serie no tenga huecos.
CREATE TABLE IF NOT EXISTS tickets_by_severity_date (
    severity          text,
    ticket_date       date,
    tickets           bigint,
    sla_breached      bigint,
    sla_breach_rate   double,
    csat_avg          double,
    run_id            text,
    PRIMARY KEY ((severity), ticket_date)
) WITH CLUSTERING ORDER BY (ticket_date DESC);

-- Q4. Revenue mensual con créditos e impuestos aplicados, normalizado a USD.
--     Partición = org_id; clustering = billing_month DESC ('yyyy-MM'): la serie mensual de una org en una partición.
CREATE TABLE IF NOT EXISTS revenue_by_org_month (
    org_id          text,
    billing_month   text,
    org_name        text,
    invoices        bigint,
    currencies      text,
    subtotal_usd    double,
    credits_usd     double,
    taxes_usd       double,
    revenue_usd     double,
    fx_applied      double,
    adjustments     bigint,
    run_id          text,
    PRIMARY KEY ((org_id), billing_month)
) WITH CLUSTERING ORDER BY (billing_month DESC);

-- Q5. Tokens GenAI y costo estimado por día.
--     Partición = org_id; clustering = usage_date DESC: el rango de fechas se lee de una sola partición.
CREATE TABLE IF NOT EXISTS genai_tokens_by_org_date (
    org_id                  text,
    usage_date              date,
    org_name                text,
    genai_tokens            bigint,
    genai_cost_usd          double,
    cost_per_1k_tokens_usd  double,
    requests                double,
    events                  bigint,
    run_id                  text,
    PRIMARY KEY ((org_id), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
