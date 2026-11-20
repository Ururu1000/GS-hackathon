-- Fenced consumer checkpoints (SPEC §2.2). The aggregator commits the next
-- Kafka offset of each partition in the same transaction as its alert writes,
-- guarded by an ownership epoch, so a replay after a crash or rebalance skips
-- what was already applied and a stale owner's write is rejected.
CREATE TABLE checkpoints (
    pipeline    text        NOT NULL,
    topic       text        NOT NULL,
    partition   integer     NOT NULL,
    epoch       bigint      NOT NULL,
    -- First offset not yet applied; -1 = nothing applied yet.
    next_offset bigint      NOT NULL DEFAULT -1,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (pipeline, topic, partition)
);
