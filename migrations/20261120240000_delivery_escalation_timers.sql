-- Escalation (SPEC §4.4): one durable timer per alert, created by the router in the same tx as
-- the alert's FIRST notification (the clock starts there), fired by the `scheduler` role.
-- `chain` is frozen at routing time: [{"recipient_id","via","channels"?}, ...]; `step` counts
-- the chain entries already fired. Semantics in crates/beacon-delivery/src/escalation.rs.
CREATE TABLE escalation_timers (
    alert_id     uuid        PRIMARY KEY REFERENCES alerts (id),
    notify_seq   integer     NOT NULL,      -- notification that started the clock; jobs reuse it
    rule_id      text        NOT NULL,
    service      text,                      -- the first notification's context, for preferences
    environment  text,                      -- re-resolved at fire time
    chain        jsonb       NOT NULL,
    step         integer     NOT NULL DEFAULT 0 CHECK (step >= 0),
    interval_ms  bigint      NOT NULL CHECK (interval_ms > 0),
    due_at       timestamptz NOT NULL,
    state        text        NOT NULL DEFAULT 'pending'
                 CHECK (state IN ('pending', 'done', 'cancelled')),
    locked_by    text,
    locked_until timestamptz,
    lease_token  uuid,
    last_note    text,                      -- why it was cancelled / skipped steps
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT escalation_timers_lease_consistent CHECK (
        (lease_token IS NULL) = (locked_until IS NULL)
    )
);

CREATE INDEX escalation_timers_due_idx ON escalation_timers (due_at) WHERE state = 'pending';
