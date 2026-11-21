-- Wake the channel workers the moment a delivery job is inserted (SPEC §9, M6 latency).
-- Statement-level, so a batch insert sends one notification; Postgres also folds identical
-- notifications within one transaction, and delivers them only on commit. Workers keep
-- polling as the fallback (and for jobs whose due_at is in the future).
CREATE FUNCTION delivery_jobs_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('beacon_delivery_jobs', '');
    RETURN NULL;
END
$$;

CREATE TRIGGER delivery_jobs_notify
    AFTER INSERT ON delivery_jobs
    FOR EACH STATEMENT EXECUTE FUNCTION delivery_jobs_notify();
