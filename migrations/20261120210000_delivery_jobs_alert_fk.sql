-- delivery_jobs.alert_id -> alerts.id, added once part (a)'s core migration
-- (20261120200000_core_alerts_outbox.sql) exists. Kept out of the create migration so that one
-- never depended on cross-branch ordering.
ALTER TABLE delivery_jobs
    ADD CONSTRAINT delivery_jobs_alert_fk FOREIGN KEY (alert_id) REFERENCES alerts (id);
