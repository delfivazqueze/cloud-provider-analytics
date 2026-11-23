-- Consultas Q1–Q5 desde AstraDB. org_ktakpuxq es la org de mayor costo en los últimos 14 días (notebook 03 §6).
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

-- Q3. Tickets críticos y tasa de SLA breach por día, últimos 30 días (al 31/08/2025, último día con tickets).
SELECT ticket_date, tickets, sla_breached, sla_breach_rate
FROM tickets_by_severity_date
WHERE severity = 'critical' AND ticket_date >= '2025-08-02' AND ticket_date <= '2025-08-31';

-- Q4. Revenue mensual con créditos e impuestos aplicados, normalizado a USD.
SELECT billing_month, currencies, subtotal_usd, credits_usd, taxes_usd, revenue_usd, fx_applied
FROM revenue_by_org_month
WHERE org_id = 'org_ktakpuxq';

-- Q5. Tokens GenAI y costo estimado por día (org_pbhsahxt: la de más tokens).
SELECT usage_date, genai_tokens, genai_cost_usd, cost_per_1k_tokens_usd
FROM genai_tokens_by_org_date
WHERE org_id = 'org_pbhsahxt' AND usage_date >= '2025-08-18' AND usage_date <= '2025-08-31';
