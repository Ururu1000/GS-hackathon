-- Durable per-channel circuit breaker (SPEC §2.3, §5.6); semantics in
-- crates/beacon-delivery/src/breaker.rs. A missing row means `closed`.
CREATE TABLE channel_breakers (
    channel              text        PRIMARY KEY,
    state                text        NOT NULL DEFAULT 'closed'
                         CHECK (state IN ('closed', 'open', 'half_open')),
    consecutive_failures integer     NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
    opened_at            timestamptz,
    open_until           timestamptz,              -- open: no claims before this
    probe_token          uuid,                     -- half_open: the worker holding the probe
    probe_until          timestamptz,              -- half_open: probe lease; after it, another
                                                   -- worker may take over the probe
    updated_at           timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT channel_breakers_open_has_deadline CHECK (state <> 'open' OR open_until IS NOT NULL),
    CONSTRAINT channel_breakers_probe_leased CHECK (
        (state = 'half_open') = (probe_token IS NOT NULL AND probe_until IS NOT NULL)
    )
);
