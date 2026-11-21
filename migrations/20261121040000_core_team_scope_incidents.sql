-- Scope active incident uniqueness and recovery watermarks by team as well as
-- (rule_id, fingerprint), preserving existing fingerprint hashes while ensuring
-- incidents and recoveries are strictly isolated across teams.

DROP INDEX IF EXISTS alerts_active_incident;
CREATE UNIQUE INDEX alerts_active_incident
    ON alerts (rule_id, team, fingerprint)
    WHERE dedup_active AND status IN ('open', 'acked');

ALTER TABLE recoveries ADD COLUMN IF NOT EXISTS team text NOT NULL DEFAULT '';
-- Backfill only when ownership is unambiguously established by alert history
-- matching the rule's current team (one distinct team in alerts for this key
-- and equal to the current rule team). Pre-fire-only or multi-team legacy
-- watermarks keep '' so they never suppress fires for another team.
UPDATE recoveries r
SET team = COALESCE(
    (SELECT min(a.team)
     FROM alerts a
     WHERE a.rule_id = r.rule_id
       AND a.fingerprint = r.fingerprint
     HAVING count(DISTINCT a.team) = 1
        AND min(a.team) = (SELECT spec->>'team' FROM rules WHERE id = r.rule_id)),
    ''
)
WHERE r.team = '';
ALTER TABLE recoveries DROP CONSTRAINT IF EXISTS recoveries_pkey;
ALTER TABLE recoveries ADD PRIMARY KEY (rule_id, team, fingerprint);
