-- Manual demo seed for the delivery pipeline (router -> worker -> email).
--
-- Run against a database where migrations ran (stub fixture or the real alerts table):
--   psql "$DATABASE_URL" -f scripts/seed_alert.sql
-- with `beacon --role router,delivery` running (SMTP_HOST/SMTP_PORT pointing at Mailpit).
--
-- Expected: within a second or two, one email per member of team `rates` (per config/teams)
-- shows up in Mailpit at http://localhost:8025, subject containing
-- "Rejected EUR 25,000,000 trade T-99877" and a body mentioning LIMIT_BREACH. The `ui`
-- channel creates no job. Re-running the script creates a NEW alert and thus new emails.
-- Check delivery state with:
--   SELECT recipient_id, status, attempts FROM delivery_jobs WHERE alert_id = '<printed id>';

BEGIN;

WITH new_alert AS (
    -- notify_seq = 1: the aggregator bumps it from 0 under the alert lock; a seed just inserts it
    -- already bumped (a CTE cannot UPDATE a row inserted by the same statement).
    INSERT INTO alerts (id, rule_id, team, fingerprint, status, severity, title, body, notify_seq)
    VALUES (
        gen_random_uuid(),
        'rates-rejected-trades',
        'rates',
        'seed-' || gen_random_uuid()::text,
        'open',
        'warning',
        'Rejected EUR 25,000,000 trade T-99877',
        E'Trade T-99877 (EUR 25,000,000) was rejected by the risk gateway.\nReason: LIMIT_BREACH',
        1
    )
    RETURNING id
)
INSERT INTO outbox (kind, alert_id, payload)
SELECT 'notify',
       n.id,
       jsonb_build_object(
           'alert_id',      n.id,
           'notify_seq',    1,
           'rule_id',       'rates-rejected-trades',
           'rule_revision', 1,
           'severity',      'warning',
           'reason',        'new',
           'route',         '[{"to":"team:rates","channels":["email","ui"]}]'::jsonb,
           'created_at',    to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
       )
  FROM new_alert n;

COMMIT;

SELECT id AS alert_id, status, severity, title, notify_seq
  FROM alerts
 WHERE rule_id = 'rates-rejected-trades'
 ORDER BY first_seen DESC
 LIMIT 1;
