-- Rule stats (GET /api/rules) aggregate alerts per rule over the last 24 h.
CREATE INDEX alerts_rule_last_seen ON alerts (rule_id, last_seen DESC);
