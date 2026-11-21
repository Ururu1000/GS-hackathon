-- Saved people/teams edits (contract v0.2.0 `savePreferences` / `saveTeam`, board
-- #172/#174): one row per edited id, applied over the startup YAML directory.
-- The YAML config stays revision 1 for every person and team; the first save writes
-- revision 2, each later save +1 (`expected_revision` compare-and-set in the API).
-- A person body is `{"preferences": [...]}`; a team body is the full effective team
-- after the save: `{"members": [...], "lead": "x"|null, "oncall": {...},
-- "default_channels": [...]}`.
CREATE TABLE directory_overrides (
    kind       text        NOT NULL CHECK (kind IN ('person', 'team')),
    id         text        NOT NULL,
    body       jsonb       NOT NULL,
    revision   bigint      NOT NULL CHECK (revision >= 2),
    updated_by text        NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, id)
);
