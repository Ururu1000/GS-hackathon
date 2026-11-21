-- M7 (SPEC §6.3): approvals can now also be decided through the signed email link
-- (`api::POST /api/approvals/email`), so `approvals.decided_via` gains 'email'.
-- The CHECK from 20261120250100 only knew 'ui' and 'sms'; without this an email
-- decision would fail its UPDATE inside `approvals::decide_in_tx`.

ALTER TABLE approvals DROP CONSTRAINT IF EXISTS approvals_decided_via_check;
ALTER TABLE approvals ADD CONSTRAINT approvals_decided_via_check
    CHECK (decided_via IN ('ui', 'sms', 'email'));
