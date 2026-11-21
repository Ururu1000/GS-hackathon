-- Manual demo seed for the delivery pipeline (router -> worker -> email), bypassing ingest.
--
-- Run against a database where migrations ran (`beacon --migrate`):
--   psql "$DATABASE_URL" -f scripts/seed_alert.sql
-- with `beacon --role router,delivery` running (SMTP_HOST/SMTP_PORT pointing at Mailpit).
--
-- Expected: within a second or two, one email per member of team `rates` (per config/teams)
-- shows up in Mailpit at http://localhost:8025, subject
-- "[WARNING] Rejected EUR 25,000,000 trade T-99877" and a body mentioning LIMIT_BREACH. The `ui`
-- channel creates no job. Re-running the script creates a NEW alert (fresh fingerprint) and thus
-- new emails. Check delivery state with:
--   SELECT recipient_id, status, attempts FROM delivery_jobs WHERE alert_id = '<printed id>';

BEGIN;

WITH new_alert AS (
    -- notify_seq = 1: the aggregator bumps it from 0 under the alert lock; a seed just inserts it
    -- already bumped (a CTE cannot UPDATE a row inserted by the same statement).
    INSERT INTO alerts (id, rule_id, rule_revision, rule_name, team, fingerprint, dedup_window_ms,
                        status, severity, color, title, body, first_seen, last_seen,
                        source, schema, notify_seq)
    VALUES (
        gen_random_uuid(),
        'rates-rejected-trades',
        1,
        'Rates: rejected trades over 10M',
        'rates',
        'seed-' || gen_random_uuid()::text,
        600000,
        'open',
        'warning',
        'amber',
        'Rejected EUR 25,000,000 trade T-99877',
        E'Trade T-99877 (EUR 25,000,000) was rejected by the risk gateway.\nReason: LIMIT_BREACH',
        now(),
        now(),
        'seed',
        'trade.rejected.v1',
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
           'team',          'rates',
           'severity',      'warning',
           'reason',        'new',
           'route',         '[{"to":"team:rates","channels":["email","ui"]}]'::jsonb,
           'created_at',    to_char(now() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
       )
  FROM new_alert n;

COMMIT;

SELECT id AS alert_id, status, severity, title, notify_seq
  FROM alerts
 WHERE rule_id = 'rates-rejected-trades' AND source = 'seed'
 ORDER BY first_seen DESC
 LIMIT 1;
