-- name: create_table_ticket :exec
CREATE TABLE IF NOT EXISTS ticket (
    reference TEXT NOT NULL UNIQUE COLLATE NOCASE,
    label TEXT,
    reason TEXT,

    -- the last time the ticket was seen by the importer
    last_seen_at TEXT,

    -- when the ticket became active (if it is)
    active_since TEXT,

    -- see app.types.Status
    status TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticket_reason ON ticket (reason);
CREATE INDEX IF NOT EXISTS idx_ticket_last_seen_at ON ticket (last_seen_at);
CREATE INDEX IF NOT EXISTS idx_ticket_active_since ON ticket (active_since);
CREATE INDEX IF NOT EXISTS idx_ticket_status ON ticket (status);

-- name: select_many_ticket :many
SELECT *
FROM ticket;

-- name: select_many_ticket_by_status :many
SELECT *
FROM ticket
WHERE status = :status;

-- name: select_many_ticket_filtered :many
-- Pass `status=None` to return every row.
SELECT *
FROM ticket
WHERE (:status IS NULL OR status = :status);

-- name: insert_many_ticket :execmany
INSERT INTO ticket (reference, label, reason, last_seen_at, active_since, status)
VALUES (:reference, :label, :reason, :last_seen_at, :active_since, :status);

-- name: insert_many_ticket_ignore_conflicts :execmany
INSERT INTO ticket (reference, label, reason, last_seen_at, active_since, status)
VALUES (:reference, :label, :reason, :last_seen_at, :active_since, :status)
ON CONFLICT(reference) DO NOTHING;

-- name: upsert_many_ticket :execmany
-- Refreshes the label, reason and last-seen time of an existing ticket.
INSERT INTO ticket (reference, label, reason, last_seen_at, active_since, status)
VALUES (:reference, :label, :reason, :last_seen_at, :active_since, :status)
ON CONFLICT(reference) DO UPDATE SET
    label = excluded.label,
    reason = excluded.reason,
    last_seen_at = excluded.last_seen_at;

-- name: archive_stale_ticket :execrows
-- `duration` is a SQLite modifier such as '-7 days'.
UPDATE ticket
SET status = 'ARCHIVED', active_since = NULL
WHERE last_seen_at < datetime('now', :duration);

-- name: activate_recent_ticket :execrows
-- Promotes the `count` most recently seen tickets: new ones become PENDING,
-- already-started ones become ACTIVE.
UPDATE ticket
SET status = iif(active_since IS NULL, 'PENDING', 'ACTIVE'),
    active_since = datetime('now')
WHERE rowid IN (
    SELECT rowid
    FROM ticket
    WHERE status != 'ARCHIVED'
    ORDER BY last_seen_at DESC
    LIMIT :count
);

-- name: archive_inactive_ticket :execrows
UPDATE ticket
SET active_since = NULL, status = 'ARCHIVED'
WHERE status NOT IN ('ACTIVE', 'PENDING') AND active_since IS NOT NULL;

-- name: update_status_ticket :execrows
UPDATE ticket
SET status = :set_status;

-- name: update_status_ticket_by_status :execrows
UPDATE ticket
SET status = :set_status
WHERE status = :where_status;

-- name: delete_ticket_by_status :execrows
DELETE FROM ticket
WHERE status = :where_status;

-- name: count_ticket_by_status :many
SELECT status, COUNT(*) AS total, MAX(last_seen_at) AS latest_seen
FROM ticket
GROUP BY status
ORDER BY status;
