-- Fake SMS/phone provider (role `gateway`): every accepted message, persisted BEFORE the gateway
-- replies and deduplicated by job id (SPEC §2.2). It models an external system, so there is no
-- FK to delivery_jobs. The UI's device simulator reads it (board #102).
CREATE TABLE gateway_receipts (
    id                 bigserial   PRIMARY KEY,
    receipt_id         uuid        NOT NULL UNIQUE DEFAULT gen_random_uuid(),
    job_id             uuid        NOT NULL UNIQUE,              -- idempotency key
    alert_id           uuid        NOT NULL,
    channel            text        NOT NULL CHECK (channel IN ('sms', 'phone')),
    recipient          text        NOT NULL,                     -- user:<id>
    to_address         text        NOT NULL,                     -- E.164
    text               text        NOT NULL,                     -- SMS text or TTS script
    approval_challenge text,                                     -- M5 (SMS reply approvals)
    duplicates         integer     NOT NULL DEFAULT 0,           -- repeat submissions of job_id
    received_at        timestamptz NOT NULL DEFAULT now(),
    last_seen_at       timestamptz NOT NULL DEFAULT now()
);

-- Simulator feed: newest first, keyset on (received_at, id).
CREATE INDEX gateway_receipts_feed_idx ON gateway_receipts (received_at DESC, id DESC);
CREATE INDEX gateway_receipts_to_idx ON gateway_receipts (to_address, received_at DESC);
