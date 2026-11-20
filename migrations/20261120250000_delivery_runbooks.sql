-- Runbooks attached to alerts (SPEC §6.1). The router renders the selected runbook from the
-- pinned event context and freezes it here in the same tx as the alert's first notification;
-- later revisions of the file never change an alert already open. Semantics:
-- crates/beacon-delivery/src/runbook.rs.
CREATE TABLE alert_runbooks (
    alert_id    uuid        PRIMARY KEY REFERENCES alerts (id),
    runbook_id  text        NOT NULL,
    revision    integer     NOT NULL,
    title       text        NOT NULL,
    format      text        NOT NULL CHECK (format IN ('yaml', 'markdown', 'link', 'text')),
    url         text,                     -- `link` runbooks only
    text        text,                     -- `markdown` / `text` original, verbatim
    steps       jsonb       NOT NULL,     -- [runbook::RenderedStep], rendered + redacted
    context     jsonb       NOT NULL,     -- the pinned template context (bounded)
    created_at  timestamptz NOT NULL DEFAULT now()
);

-- Manual steps ticked in the UI (`POST /api/alerts/{id}/steps/{step}/check`).
CREATE TABLE step_marks (
    alert_id   uuid        NOT NULL REFERENCES alerts (id),
    step_id    text        NOT NULL,
    done_by    text        NOT NULL,
    done_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (alert_id, step_id)
);
