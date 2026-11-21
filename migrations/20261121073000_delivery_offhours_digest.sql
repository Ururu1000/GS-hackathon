-- Off-hours digest (crates/beacon-delivery/src/offhours.rs). A person's preference can say
-- `otherwise: [digest]`: outside their `when` window, a non-critical notification is held here
-- instead of becoming a delivery job, and every held row of that person whose `release_at` has
-- passed is sent as ONE email ("overnight: 3 alerts, 1 resolved"). Nothing is dropped: a row
-- leaves this table's pending set only in the transaction that files its digest job.
CREATE TABLE offhours_held (
    id           bigserial   PRIMARY KEY,
    recipient_id text        NOT NULL,
    alert_id     uuid        NOT NULL REFERENCES alerts (id),
    notify_seq   integer     NOT NULL,
    rule_id      text        NOT NULL,
    address      text        NOT NULL,              -- the person's email, frozen at routing time
    route_info   jsonb       NOT NULL,              -- provenance as the router decided it
    release_at   timestamptz NOT NULL,              -- the preference window's next start
    created_at   timestamptz NOT NULL DEFAULT now(),
    released_at  timestamptz,                       -- set when a digest covered this row
    digest_alert uuid,                              -- alert the digest job was filed under
    UNIQUE (alert_id, recipient_id, notify_seq)
);

CREATE INDEX offhours_held_due_idx
    ON offhours_held (release_at, recipient_id) WHERE released_at IS NULL;
CREATE INDEX offhours_held_alert_idx ON offhours_held (alert_id);
