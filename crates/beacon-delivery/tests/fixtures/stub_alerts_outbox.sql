-- TEST-ONLY stub of part (a)'s alerts + outbox tables (board #35, #72), used until the real
-- base migration lands in migrations/. IF NOT EXISTS makes this a no-op once it does.
-- Only the columns delivery reads or writes are here. Delete this file when the real one lands.

CREATE TABLE IF NOT EXISTS alerts (
    id          uuid        PRIMARY KEY,
    rule_id     text        NOT NULL,
    team        text        NOT NULL DEFAULT 'rates',
    fingerprint text        NOT NULL DEFAULT '',
    status      text        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acked', 'resolved')),
    severity    text        NOT NULL DEFAULT 'warning' CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    title       text        NOT NULL,
    body        text        NOT NULL DEFAULT '',
    count       bigint      NOT NULL DEFAULT 1,
    notify_seq  integer     NOT NULL DEFAULT 0,
    version     bigint      NOT NULL DEFAULT 1,
    first_seen  timestamptz NOT NULL DEFAULT now(),
    last_seen   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial   PRIMARY KEY,
    kind         text        NOT NULL,
    alert_id     uuid        NOT NULL REFERENCES alerts (id),
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);

CREATE INDEX IF NOT EXISTS outbox_unprocessed_idx ON outbox (kind, id) WHERE processed_at IS NULL;
