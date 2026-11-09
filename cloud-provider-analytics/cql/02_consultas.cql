-- Consultas mínimas de la 2ª entrega. org_ktakpuxq es la org de mayor costo en los últimos 14 días (notebook 02 §6).
-- En la CQL Console de Astra: ejecutar primero el USE.

USE cloud_analytics;

-- Q1. Costos y requests diarios de una organización por servicio, entre dos fechas (lee 1 partición).
SELECT usage_date, service, daily_cost_usd, requests, events, anomaly_events
FROM org_daily_usage_by_service
WHERE org_id = 'org_ktakpuxq'
  AND usage_date >= '2025-08-18' AND usage_date <= '2025-08-31';

-- Q2. Top-3 servicios por costo de los últimos 14 días al 31/08/2025 (lee 1 partición, ya ordenada).
SELECT rank, service, cost_14d_usd, requests_14d
FROM top_services_14d
WHERE org_id = 'org_ktakpuxq' AND as_of_date = '2025-08-31'
LIMIT 3;
