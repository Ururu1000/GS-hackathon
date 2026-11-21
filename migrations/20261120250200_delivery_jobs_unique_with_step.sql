-- The idempotency key must distinguish jobs that page the same person on the same channel for the
-- same notification but for a different reason: an escalation step (1..), the runbook `escalate`
-- action (100+n) or an approval prompt (200+n). Before this, such a job was silently dropped by
-- ON CONFLICT DO NOTHING (or had to borrow a fake notify_seq, which could swallow a later real
-- renotify). Plain notification jobs use escalation_step = 0.
UPDATE delivery_jobs SET escalation_step = 0 WHERE escalation_step IS NULL;

ALTER TABLE delivery_jobs
    ALTER COLUMN escalation_step SET DEFAULT 0,
    ALTER COLUMN escalation_step SET NOT NULL;

ALTER TABLE delivery_jobs DROP CONSTRAINT delivery_jobs_idempotency;
ALTER TABLE delivery_jobs ADD CONSTRAINT delivery_jobs_idempotency
    UNIQUE (alert_id, recipient_id, channel, notify_seq, escalation_step);
