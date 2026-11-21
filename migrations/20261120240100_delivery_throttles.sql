-- Throttles (SPEC §4.3, §5.4, §13): a token bucket per (rule, recipient), debited once per
-- recipient notification inside the router tx. Over the limit, the planned jobs are parked in
-- throttle_suppressed instead; a digest ("N more suppressed") is sent when tokens return.
-- Nothing is silently dropped. Semantics in crates/beacon-delivery/src/throttle.rs.
CREATE TABLE throttle_buckets (
    rule_id        text             NOT NULL,
    recipient_id   text             NOT NULL,
    tokens         double precision NOT NULL CHECK (tokens >= 0),
    capacity       integer          NOT NULL CHECK (capacity > 0),
    refill_per_sec double precision NOT NULL CHECK (refill_per_sec > 0),
    refilled_at    timestamptz      NOT NULL DEFAULT now(),
    updated_at     timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (rule_id, recipient_id)
);

CREATE TABLE throttle_suppressed (
    id           bigserial   PRIMARY KEY,
    rule_id      text        NOT NULL,
    recipient_id text        NOT NULL,
    alert_id     uuid        NOT NULL REFERENCES alerts (id),
    notify_seq   integer     NOT NULL,
    jobs         jsonb       NOT NULL,      -- [{"channel","address","route_info"}] as planned
    created_at   timestamptz NOT NULL DEFAULT now(),
    digested_at  timestamptz,               -- set when a digest covered this row
    digest_alert uuid                       -- alert id the digest jobs were filed under
);

CREATE INDEX throttle_suppressed_pending_idx
    ON throttle_suppressed (rule_id, recipient_id, id) WHERE digested_at IS NULL;
