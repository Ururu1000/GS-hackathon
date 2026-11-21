-- Runbook action executions (SPEC §6.2) and approvals (§6.3). Semantics:
-- crates/beacon-delivery/src/actions.rs and src/approvals.rs.

-- One execution = one attempt-set of one action for one alert step, with frozen params.
-- Leased by the `actions` role; the API only inserts `queued` rows and returns 202.
CREATE TABLE action_executions (
    id            uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id      uuid        NOT NULL REFERENCES alerts (id),
    step_id       text        NOT NULL,
    action        text        NOT NULL,
    params        jsonb       NOT NULL,
    state         text        NOT NULL DEFAULT 'queued'
                  CHECK (state IN ('queued', 'running', 'ok', 'failed', 'uncertain')),
    requested_by  text        NOT NULL,
    approval_id   uuid,
    attempts      integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    locked_by     text,
    locked_until  timestamptz,
    lease_token   uuid,
    started_at    timestamptz,
    finished_at   timestamptz,
    output        text,                   -- short, redacted
    created_at    timestamptz NOT NULL DEFAULT now(),
    updated_at    timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT action_executions_lease_consistent CHECK (
        (state = 'running') = (lease_token IS NOT NULL AND locked_until IS NOT NULL)
    )
);

-- At most one live execution per (alert, step): a second run is 409 invalid_transition.
CREATE UNIQUE INDEX action_executions_live_idx
    ON action_executions (alert_id, step_id) WHERE state IN ('queued', 'running');
CREATE INDEX action_executions_queue_idx ON action_executions (created_at) WHERE state = 'queued';
CREATE INDEX action_executions_alert_idx ON action_executions (alert_id, step_id, created_at);

-- One approval request per (alert, step) at a time. The request freezes the action and params;
-- approving it consumes it and queues the execution in ONE tx.
CREATE TABLE approvals (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    alert_id       uuid        NOT NULL REFERENCES alerts (id),
    step_id        text        NOT NULL,
    action         text        NOT NULL,
    params         jsonb       NOT NULL,
    eligible       text[]      NOT NULL,  -- user:<id>
    state          text        NOT NULL DEFAULT 'pending'
                   CHECK (state IN ('pending', 'approved', 'rejected', 'expired')),
    requested_by   text        NOT NULL,
    requested_at   timestamptz NOT NULL DEFAULT now(),
    expires_at     timestamptz NOT NULL,
    sms_code_hash  text        NOT NULL,  -- sha256(approval id || code), never the code itself
    sms_attempts   integer     NOT NULL DEFAULT 0 CHECK (sms_attempts >= 0),
    decided_by     text,
    decided_via    text        CHECK (decided_via IN ('ui', 'sms')),
    decided_at     timestamptz,
    note           text,
    execution_id   uuid        REFERENCES action_executions (id),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX approvals_pending_idx ON approvals (alert_id, step_id) WHERE state = 'pending';

-- Transitions only (SPEC §4.5): who did what, through which channel. No raw bodies, no secrets.
-- Feeds AlertDetail.timeline (kinds step_run, step_done, approval, escalated, acked, ...).
CREATE TABLE audit_log (
    id        bigserial   PRIMARY KEY,
    at        timestamptz NOT NULL DEFAULT now(),
    actor     text        NOT NULL,       -- user:<id> | system
    via       text        NOT NULL,       -- ui | sms | system
    alert_id  uuid        REFERENCES alerts (id),
    kind      text        NOT NULL,
    message   text        NOT NULL,
    detail    jsonb       NOT NULL DEFAULT '{}'
);

CREATE INDEX audit_log_alert_idx ON audit_log (alert_id, at);
