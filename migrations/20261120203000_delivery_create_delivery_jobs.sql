-- Delivery jobs (part (b)). One row per (alert, recipient, channel, notification).
-- Written by the router in the same tx that marks the outbox 'notify' row processed;
-- leased and finalized by channel workers (SPEC §2.2, board #27/#35).
--
-- No FK to alerts(id) yet: alerts is created by part (a)'s base migration and this file must
-- not depend on migration ordering between teammates. A later migration adds the FK.
--
-- Later features plug in without changing this table:
--   sms / phone      -> new `channel` values
--   fallback         -> fallback_of (one hop)
--   escalation       -> escalation_step
--   throttles        -> decided by the router before insert
--   approvals        -> their own tables

CREATE TABLE delivery_jobs (
    id               uuid        PRIMARY KEY DEFAULT gen_random_uuid(), -- also SMTP Message-ID
    alert_id         uuid        NOT NULL,
    notify_seq       integer     NOT NULL,
    recipient_id     text        NOT NULL,
    channel          text        NOT NULL,
    address          text        NOT NULL,                 -- frozen at routing time
    route_info       jsonb       NOT NULL DEFAULT '{}',    -- provenance: route target, rule, reason
    status           text        NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'in_flight', 'sent', 'cancelled', 'dead')),
    attempts         integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     integer     NOT NULL DEFAULT 8 CHECK (max_attempts > 0),
    due_at           timestamptz NOT NULL DEFAULT now(),
    locked_by        text,
    locked_until     timestamptz,
    lease_token      uuid,
    last_error       text,
    last_error_class text        CHECK (last_error_class IN ('transient', 'permanent', 'ambiguous')),
    provider_receipt jsonb,
    sent_at          timestamptz,
    fallback_of      uuid        REFERENCES delivery_jobs (id),
    escalation_step  integer,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT delivery_jobs_idempotency UNIQUE (alert_id, recipient_id, channel, notify_seq),
    CONSTRAINT delivery_jobs_lease_consistent CHECK (
        (status = 'in_flight') = (lease_token IS NOT NULL AND locked_until IS NOT NULL)
    )
);

-- Due pending jobs per channel (claim).
CREATE INDEX delivery_jobs_due_idx ON delivery_jobs (channel, due_at) WHERE status = 'pending';
-- Expired leases per channel (reclaim / dead-letter).
CREATE INDEX delivery_jobs_lease_idx ON delivery_jobs (channel, locked_until) WHERE status = 'in_flight';
-- Alert detail: deliveries[] for one alert.
CREATE INDEX delivery_jobs_alert_idx ON delivery_jobs (alert_id);
