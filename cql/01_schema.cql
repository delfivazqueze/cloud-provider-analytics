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
