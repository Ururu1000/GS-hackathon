-- Rules live in Postgres (SPEC §4.6): the API and the YAML seed write through
-- one validator; matchers reload when config_state.revision changes.
CREATE TABLE rules (
    id         text        PRIMARY KEY,
    spec       jsonb       NOT NULL,
    -- Per-rule revision for compare-and-set. A deleted rule keeps its row, so
    -- recreating the same id continues from a higher revision.
    revision   bigint      NOT NULL,
    deleted    boolean     NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text        NOT NULL
);

CREATE TABLE config_state (
    key      text   PRIMARY KEY,
    revision bigint NOT NULL
);
INSERT INTO config_state (key, revision) VALUES ('rules', 0);

-- Recent event samples (rule test panel, unknown-schema discovery). Bounded:
-- matchers sample at most one event per schema every couple of seconds and
-- prune old rows.
CREATE TABLE event_samples (
    id          bigserial   PRIMARY KEY,
    event_id    text        NOT NULL,
    source      text        NOT NULL,
    schema      text        NOT NULL,
    received_at timestamptz NOT NULL,
    fields      jsonb       NOT NULL DEFAULT '{}',
    raw         jsonb       NOT NULL
);
CREATE INDEX event_samples_schema ON event_samples (schema, id DESC);
