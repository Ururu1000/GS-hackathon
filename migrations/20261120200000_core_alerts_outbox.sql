-- Core tables owned by ingest/engine (a): alerts and the transactional outbox.
-- Delivery (b) reads outbox kind='notify'; (a) relays kind='alert_update' to Kafka alerts.updates.

CREATE TABLE alerts (
    id               uuid PRIMARY KEY,
    rule_id          text        NOT NULL,
    rule_revision    bigint      NOT NULL,
    rule_name        text        NOT NULL,
    team             text        NOT NULL,
    fingerprint      text        NOT NULL,
    generation       integer     NOT NULL DEFAULT 1,
    dedup_active     boolean     NOT NULL DEFAULT true,
    dedup_window_ms  bigint      NOT NULL,
    status           text        NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'acked', 'resolved')),
    severity         text        NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    color            text        NOT NULL,
    title            text        NOT NULL,
    body             text        NOT NULL DEFAULT '',
    fields           jsonb       NOT NULL DEFAULT '[]',
    count            bigint      NOT NULL DEFAULT 1,
    first_seen       timestamptz NOT NULL,
    last_seen        timestamptz NOT NULL,
    -- Bumped on every visible change (count, status, re-render).
    version          bigint      NOT NULL DEFAULT 1,
    -- Version of the last status change; ack/resolve conflict only against this.
    status_version   bigint      NOT NULL DEFAULT 1,
    -- Incremented under the row lock each time a notify outbox row is written.
    notify_seq       integer     NOT NULL DEFAULT 0,
    runbook_revision bigint,
    source           text        NOT NULL,
    schema           text        NOT NULL,
    last_event_id    text,
    acked_by         text,
    acked_at         timestamptz,
    resolved_by      text,
    resolved_at      timestamptz,
    resolved_reason  text
);

-- At most one active incident per (rule, fingerprint). Same predicate as the aggregator's lookup.
CREATE UNIQUE INDEX alerts_active_incident
    ON alerts (rule_id, fingerprint)
    WHERE dedup_active AND status IN ('open', 'acked');

-- Feed: per team, newest first, keyset on (first_seen, id).
CREATE INDEX alerts_feed ON alerts (team, first_seen DESC, id DESC);
CREATE INDEX alerts_status ON alerts (team, status, first_seen DESC);

CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    kind         text        NOT NULL CHECK (kind IN ('notify', 'alert_update')),
    alert_id     uuid        NOT NULL REFERENCES alerts (id),
    payload      jsonb       NOT NULL DEFAULT '{}',
    created_at   timestamptz NOT NULL DEFAULT now(),
    processed_at timestamptz
);

CREATE INDEX outbox_pending ON outbox (kind, id) WHERE processed_at IS NULL;
