-- auto_resolve.quiet_for (SPEC §4.3): an open/acked alert with no new match for
-- this long is resolved by the quiet-timer sweeper.
ALTER TABLE alerts ADD COLUMN quiet_for_ms bigint;
CREATE INDEX alerts_quiet ON alerts (last_seen) WHERE quiet_for_ms IS NOT NULL AND status IN ('open', 'acked');
