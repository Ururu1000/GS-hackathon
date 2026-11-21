-- Grouped rules: the group tuple without the time bucket, so a recovery
-- signal can resolve every open digest of that tuple, whatever its window.
ALTER TABLE alerts ADD COLUMN group_key text;
CREATE INDEX alerts_group_open ON alerts (rule_id, group_key) WHERE group_key IS NOT NULL AND status IN ('open', 'acked');
