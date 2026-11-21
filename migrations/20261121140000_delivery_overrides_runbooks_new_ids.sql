-- Runtime config (audit 2026-11-21): `directory_overrides` also stores
--   * runbooks imported through the UI (`POST /api/runbooks`), kind 'runbook', and
--   * people and teams CREATED at runtime (`POST /api/people`, `POST /api/teams`), not only
--     edits of ids from the YAML base.
-- A created id starts at revision 1 (the YAML base keeps its implicit revision 1 and its
-- first edit still writes 2). The body shapes are documented in crates/beacon-delivery/src/overrides.rs.
-- Every reader (router, scheduler, actions, API) already rebuilds its directory when the
-- table's (count, sum(revision)) stamp moves, so new rows apply without a restart.

ALTER TABLE directory_overrides DROP CONSTRAINT IF EXISTS directory_overrides_kind_check;
ALTER TABLE directory_overrides ADD CONSTRAINT directory_overrides_kind_check
    CHECK (kind IN ('person', 'team', 'runbook'));

ALTER TABLE directory_overrides DROP CONSTRAINT IF EXISTS directory_overrides_revision_check;
ALTER TABLE directory_overrides ADD CONSTRAINT directory_overrides_revision_check
    CHECK (revision >= 1);
