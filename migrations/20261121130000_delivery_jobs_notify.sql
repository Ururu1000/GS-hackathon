-- Wake the channel workers the moment a delivery job is inserted (SPEC §9, M6 latency).
-- Statement-level: one notification per delivery channel per statement, with the channel
-- name as the payload, so only that channel's workers wake. Postgres also folds identical
-- notifications within one transaction and delivers them only on commit. Workers keep
-- polling as the fallback (and for jobs whose due_at is in the future).
CREATE FUNCTION delivery_jobs_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('beacon_delivery_jobs', c.channel)
    FROM (SELECT DISTINCT channel FROM new_jobs) c;
    RETURN NULL;
END
$$;

CREATE TRIGGER delivery_jobs_notify
    AFTER INSERT ON delivery_jobs
    REFERENCING NEW TABLE AS new_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION delivery_jobs_notify();
