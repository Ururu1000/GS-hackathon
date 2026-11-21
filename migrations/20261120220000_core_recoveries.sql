-- Latest recovery per incident key. Fire and recovery events have different
-- event ids, so they can reach the aggregator out of order; a fire older than
-- the recorded recovery does not reopen the incident.
CREATE TABLE recoveries (
    rule_id      text        NOT NULL,
    fingerprint  text        NOT NULL,
    recovered_at timestamptz NOT NULL,
    PRIMARY KEY (rule_id, fingerprint)
);
