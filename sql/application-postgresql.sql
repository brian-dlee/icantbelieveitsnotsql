-- Application query patterns: PostgreSQL 16
--
-- Each query below reproduces a *shape* found in a production Python service
-- that talks to PostgreSQL 16 through asyncpg ($n placeholders). Names are
-- generic; the structure, functions, parameter positions, and typing hazards
-- are the point. butter should eventually generate correct code for every
-- query here.
--
-- Patterns the service builds dynamically in Python are listed at the end;
-- the README's "Queries that would otherwise be built at runtime" gives the
-- static form butter supports for each.

-- ------------------------------------------------------------------ schema

-- Conventions mirrored from the service schema: bigserial and identity keys,
-- timestamptz everywhere, jsonb payloads, CHECK constraints instead of enum
-- types, partial indexes, and a DISTINCT ON view.

CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY,
    topic TEXT NOT NULL,
    stream_key TEXT NOT NULL,
    payload JSONB NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
    status VARCHAR(16) NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX outbox_pending ON outbox (available_at) WHERE status = 'pending';

CREATE TABLE outbox_streams (
    stream_key TEXT PRIMARY KEY,
    locked_by TEXT,
    last_sent_id BIGINT
);

CREATE TABLE worker_state (
    worker_name TEXT PRIMARY KEY,
    enabled BOOLEAN NOT NULL DEFAULT false,
    schedule JSONB,
    runtime JSONB,
    run_count BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE sync_history (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    profile_key TEXT NOT NULL,
    channel VARCHAR(32) NOT NULL,
    opted_in BOOLEAN NOT NULL,
    amount NUMERIC,
    observed_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE audit_entries (
    id BIGSERIAL PRIMARY KEY,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE VIEW latest_sync AS
SELECT DISTINCT ON (profile_key) *
FROM sync_history
ORDER BY profile_key, observed_at DESC, id DESC;

-- ----------------------------------------------------------------- upserts

-- jsonb parameter cast, NOW(), DO UPDATE from EXCLUDED.
-- name: upsert_worker :exec
INSERT INTO worker_state (worker_name, enabled, schedule, updated_at)
VALUES ($1, $2, $3::jsonb, NOW())
ON CONFLICT (worker_name) DO UPDATE
SET enabled = EXCLUDED.enabled,
    schedule = EXCLUDED.schedule,
    updated_at = EXCLUDED.updated_at;

-- Guarded update action and RETURNING.
-- name: claim_worker_runtime :one
INSERT INTO worker_state (worker_name, runtime)
VALUES ($1, $2::jsonb)
ON CONFLICT (worker_name) DO UPDATE
SET runtime = EXCLUDED.runtime
WHERE worker_state.runtime IS NULL OR worker_state.updated_at < $3::timestamptz
RETURNING worker_name, runtime, updated_at;

-- name: ensure_stream :exec
INSERT INTO outbox_streams (stream_key) VALUES ($1)
ON CONFLICT (stream_key) DO NOTHING;

-- Aliased target, jsonb concatenation with jsonb_build_object.
-- name: merge_worker_runtime :exec
INSERT INTO worker_state AS w (worker_name, runtime)
VALUES ($1, jsonb_build_object('last_error', $2::text))
ON CONFLICT (worker_name) DO UPDATE
SET runtime = COALESCE(w.runtime, '{}'::jsonb) || jsonb_build_object('last_error', $2::text);

-- ------------------------------------------------------------------ writes

-- Assumes an outbox_streams row already exists for the stream_key:
-- claim_pending inner-joins outbox_streams and skips outbox rows without one.
-- name: enqueue :one
INSERT INTO outbox (topic, stream_key, payload, attributes)
VALUES ($1, $2, $3::jsonb, $4::jsonb)
RETURNING id;

-- name: enqueue_many :execmany
INSERT INTO outbox (topic, stream_key, payload)
VALUES ($1, $2, $3::jsonb);

-- Arithmetic and a seconds parameter turned into an interval.
-- name: retry_later :execrows
UPDATE outbox
SET attempts = attempts + 1,
    status = 'pending',
    available_at = now() + ($2 || ' seconds')::interval
WHERE id = $1;

-- Array parameter.
-- name: mark_sent :many
UPDATE outbox
SET status = 'sent', sent_at = now()
WHERE id = ANY($1::bigint[])
RETURNING id, stream_key;

-- Compare-and-set on a jsonb value; caller reads the affected-row count.
-- Plain `=` mirrors the production shape: a NULL expected value never matches.
-- name: swap_schedule :execrows
UPDATE worker_state
SET schedule = $3::jsonb, run_count = run_count + 1
WHERE worker_name = $1 AND schedule = $2::jsonb;

-- name: record_audit :exec
INSERT INTO audit_entries (actor, action, details) VALUES ($1, $2, $3::jsonb);

-- ------------------------------------------------------------------- reads

-- Plain row lock on a single row.
-- name: lock_worker :one
SELECT worker_name, enabled, schedule, runtime
FROM worker_state
WHERE worker_name = $1
FOR UPDATE;

-- Claim by stream: locks only the stream row `s` (not the outbox rows),
-- with SKIP LOCKED placed before LIMIT.
-- name: claim_pending :many
SELECT o.id, o.topic, o.stream_key, o.payload, o.attributes
FROM outbox o
JOIN outbox_streams s ON s.stream_key = o.stream_key
WHERE o.status = 'pending' AND o.available_at <= now()
ORDER BY o.id
FOR UPDATE OF s SKIP LOCKED
LIMIT $1;

-- Optional filters via `$n::type IS NULL OR ...`, keyset pagination,
-- repeated parameter, IN with literals.
-- name: list_outbox :many
SELECT id, topic, status, attempts, created_at
FROM outbox
WHERE ($1::text IS NULL OR topic = $1)
  AND ($2::text IS NULL OR status = $2)
  AND status IN ('pending', 'failed')
  AND ($3::bigint IS NULL OR id < $3::bigint)
ORDER BY id DESC
LIMIT $4;

-- ANY over a text array parameter, LEFT JOIN ... USING, prefixed aliases.
-- name: list_streams :many
SELECT
    s.stream_key AS s_stream_key,
    s.locked_by AS s_locked_by,
    o.id AS o_id,
    o.status AS o_status
FROM outbox_streams s
LEFT JOIN outbox o USING (stream_key)
WHERE s.stream_key = ANY($1::text[]);

-- FILTER aggregates, CASE, jsonb field cast, scalar subquery in ORDER BY.
-- name: outbox_stats :many
SELECT
    topic,
    count(*) AS total,
    count(*) FILTER (WHERE status = 'failed') AS failed,
    min(created_at) FILTER (WHERE status = 'pending') AS oldest_pending,
    CASE WHEN count(*) > 100 THEN 'busy' ELSE 'quiet' END AS load,
    max((attributes->>'priority')::bigint) AS max_priority
FROM outbox
GROUP BY topic
ORDER BY (SELECT min(id) FROM outbox o2 WHERE o2.topic = outbox.topic);

-- EXISTS and casts of a parameter to timestamptz.
-- name: has_recent_failures :one
SELECT EXISTS (
    SELECT 1 FROM outbox
    WHERE status = 'failed' AND created_at > $1::timestamptz
) AS has_failures;

-- Recursive CTE emulating a loose index scan over distinct values.
-- name: distinct_topics :many
WITH RECURSIVE topics AS (
    (SELECT topic FROM outbox ORDER BY topic LIMIT 1)
    UNION ALL
    SELECT (SELECT o.topic FROM outbox o WHERE o.topic > t.topic ORDER BY o.topic LIMIT 1)
    FROM topics t
    WHERE t.topic IS NOT NULL
)
SELECT topic FROM topics WHERE topic IS NOT NULL;

-- DISTINCT ON CTEs combined with FULL OUTER JOIN.
-- name: latest_by_channel :many
WITH email AS (
    SELECT DISTINCT ON (profile_key) *
    FROM sync_history WHERE channel = 'email'
    ORDER BY profile_key, observed_at DESC, id DESC
), sms AS (
    SELECT DISTINCT ON (profile_key) *
    FROM sync_history WHERE channel = 'sms'
    ORDER BY profile_key, observed_at DESC, id DESC
), push AS (
    SELECT DISTINCT ON (profile_key) *
    FROM sync_history WHERE channel = 'push'
    ORDER BY profile_key, observed_at DESC, id DESC
)
SELECT
    COALESCE(email.profile_key, sms.profile_key, push.profile_key) AS profile_key,
    email.opted_in AS email_opted_in,
    sms.opted_in AS sms_opted_in,
    push.opted_in AS push_opted_in
FROM email
FULL OUTER JOIN sms ON sms.profile_key = email.profile_key
FULL OUTER JOIN push ON push.profile_key = COALESCE(email.profile_key, sms.profile_key);

-- Selecting from the DISTINCT ON view.
-- name: latest_sync_for :one
SELECT profile_key, channel, opted_in, observed_at
FROM latest_sync
WHERE profile_key = $1;

-- --------------------------------------------------------------- session

-- name: set_lock_timeout :exec
SET LOCAL lock_timeout = '5s';

-- ------------------------------------------------------------ dynamic SQL
--
-- Built in Python today; the postgresql_static_idioms corpus case shows the
-- main static forms (optional filters with casts, set_config for SET LOCAL).
-- 1. Condition lists joined with " AND " where each condition numbers its
--    parameter as `${len(params)}`.
-- 2. Column names interpolated into `SET {col} = EXCLUDED.{col}` and a
--    shared `SELECT {COLUMNS}` constant.
-- 3. `SET LOCAL lock_timeout = '{value}'` with an interpolated value.
--
-- Result-handling notes: asyncpg's execute() returns a status string such as
-- "UPDATE 1" (the service parses it for :execrows), jsonb arrives as str
-- unless a codec is registered, and rows are Record objects addressed by name.
