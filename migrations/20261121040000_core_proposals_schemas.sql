-- Schema mappings live in Postgres like rules (seeded from config/schemas),
-- so the UI can preview and publish mappings for unknown shapes live.
CREATE TABLE schema_mappings (
    schema     text        PRIMARY KEY,
    spec       jsonb       NOT NULL,
    revision   bigint      NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    updated_by text        NOT NULL
);

-- "Suggest change" proposals against a rule revision (SPEC §6.5). Accepting
-- publishes proposed_rule with base_revision as the expected revision.
CREATE TABLE proposals (
    id            uuid        PRIMARY KEY,
    rule_id       text        NOT NULL,
    rule_name     text        NOT NULL,
    team          text        NOT NULL,
    kind          text        NOT NULL CHECK (kind IN ('noisy', 'reroute', 'template', 'other')),
    summary       text        NOT NULL,
    note          text        NOT NULL DEFAULT '',
    base_revision bigint      NOT NULL,
    base_rule     jsonb       NOT NULL,
    proposed_rule jsonb       NOT NULL,
    author        text        NOT NULL,
    auto          boolean     NOT NULL DEFAULT false,
    alert_id      uuid,
    created_at    timestamptz NOT NULL DEFAULT now(),
    state         text        NOT NULL DEFAULT 'pending' CHECK (state IN ('pending', 'accepted', 'rejected', 'stale')),
    decided_by    text,
    decided_at    timestamptz
);
CREATE INDEX proposals_team_state ON proposals (team, state, created_at DESC);
