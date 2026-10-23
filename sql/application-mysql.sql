-- Application query patterns: MySQL 5.7
--
-- Each query below reproduces a *shape* found in a production Python service
-- that talks to MySQL 5.7 through PyMySQL/aiomysql. Names are generic; the
-- structure, functions, parameter positions, and typing hazards are the point.
-- butter should eventually generate correct code for every query here.
--
-- Placeholder note: the service writes `%(name)s` (pyformat). Here they are
-- written as butter's `:name`, and `%s` as `?`.
--
-- Patterns the service builds dynamically in Python have no static equivalent
-- yet and are listed at the end of the file as open design questions.

-- ------------------------------------------------------------------ schema

-- Conventions mirrored from the service schema:
-- camelCase columns, INT AUTO_INCREMENT keys, DATETIME(3) audit columns with
-- ON UPDATE, TINYINT(1) booleans, TINYINT(4) flags/status numbers, FLOAT and
-- DOUBLE money, ENUM status, JSON payloads, VARBINARY ids.

CREATE TABLE stores (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    timezone VARCHAR(64) NOT NULL,
    active TINYINT(1) NOT NULL DEFAULT 1,
    createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

CREATE TABLE accounts (
    id INT NOT NULL AUTO_INCREMENT,
    email VARCHAR(255) NOT NULL,
    firstName VARCHAR(100),
    lastName VARCHAR(100),
    homeStoreId INT,
    banned TINYINT(4) NOT NULL DEFAULT 0,
    verified TINYINT(1) NOT NULL DEFAULT 0,
    status ENUM('active', 'suspended', 'closed') NOT NULL DEFAULT 'active',
    preferences JSON,
    externalId VARBINARY(16),
    lastSeenAt DATETIME(0),
    createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updatedAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY accounts_email (email)
);

CREATE TABLE listings (
    id INT NOT NULL AUTO_INCREMENT,
    storeId INT NOT NULL,
    title VARCHAR(255) NOT NULL,
    description MEDIUMTEXT,
    retailPrice DOUBLE,
    currentPrice FLOAT NOT NULL DEFAULT 0,
    taxRate DECIMAL(10, 2) NOT NULL DEFAULT 0,
    state TINYINT(4) NOT NULL DEFAULT 0,
    closesAt DATETIME(0) NOT NULL,
    closedAt DATETIME(3),
    attributes JSON,
    createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updatedAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

CREATE TABLE offers (
    id INT NOT NULL AUTO_INCREMENT,
    listingId INT NOT NULL,
    accountId INT NOT NULL,
    amount FLOAT NOT NULL,
    maxAmount FLOAT,
    winning TINYINT(1) NOT NULL DEFAULT 0,
    createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

CREATE TABLE pickups (
    id INT NOT NULL AUTO_INCREMENT,
    accountId INT NOT NULL,
    storeId INT NOT NULL,
    scheduledFor DATETIME(0) NOT NULL,
    arrivedAt DATETIME(0),
    missed TINYINT(1) NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

CREATE TABLE pickup_listings (
    pickupId INT NOT NULL,
    listingId INT NOT NULL,
    PRIMARY KEY (pickupId, listingId)
);

CREATE TABLE account_counters (
    accountId INT NOT NULL,
    counterKey VARCHAR(64) NOT NULL,
    hits INT NOT NULL DEFAULT 0,
    lastValue DOUBLE,
    updatedAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (accountId, counterKey)
);

CREATE TABLE event_log (
    id INT NOT NULL AUTO_INCREMENT,
    listingId INT NOT NULL,
    action VARCHAR(32) NOT NULL,
    note TEXT,
    createdAt DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
);

-- ------------------------------------------------- reads: single table/joins

-- camelCase -> snake_case aliases that match a pydantic model; mixed-case AS.
-- name: get_account :one
SELECT
    a.id AS account_id,
    a.email AS email,
    a.firstName as first_name,
    a.lastName AS last_name,
    a.banned AS banned,
    a.verified AS verified,
    a.status AS status,
    a.preferences AS preferences,
    a.createdAt AS created_at
FROM accounts a
WHERE a.id = :account_id;

-- Positional parameter.
-- name: get_account_email :one
SELECT email FROM accounts WHERE id = ?;

-- LEFT JOIN nullability, table.* expansion, inline comment.
-- name: get_listing_with_store :one
SELECT
    l.*,
    -- the store may have been removed
    s.name AS store_name,
    s.timezone AS store_timezone
FROM listings l
LEFT JOIN stores s ON s.id = l.storeId
WHERE l.id = :listing_id;

-- Replication shape: literal tag aliased to a reserved word, backticked
-- table, keyset cursor, LIMIT parameter.
-- name: replicate_listings :many
SELECT
    'listings' AS `table`,
    id AS pk,
    `listings`.*
FROM `listings`
WHERE id > :last_id
ORDER BY id
LIMIT :batch_size;

-- Backticked alias containing spaces.
-- name: export_accounts :many
SELECT a.id AS `Account Id`, a.email AS `Email Address`
FROM accounts a
ORDER BY a.id DESC;

-- Several joins, extra predicates and a parameter inside the join condition.
-- name: list_open_offers_for_account :many
SELECT
    o.id AS offer_id,
    o.amount AS amount,
    l.title AS title,
    s.name AS store_name
FROM offers o
JOIN listings l ON l.id = o.listingId AND l.closesAt > NOW()
INNER JOIN stores s ON s.id = l.storeId AND s.active = 1
LEFT JOIN pickup_listings pl ON pl.listingId = l.id
WHERE o.accountId = :account_id
  AND pl.pickupId IS NULL
ORDER BY l.closesAt ASC, o.id DESC;

-- RIGHT JOIN.
-- name: list_stores_with_listing_counts :many
SELECT s.id AS store_id, COUNT(l.id) AS listing_count
FROM listings l
RIGHT JOIN stores s ON s.id = l.storeId
GROUP BY s.id;

-- List parameter expanded by the driver: `IN %(ids)s` with a tuple.
-- name: list_listings_by_ids :many
SELECT id, title, currentPrice AS current_price
FROM listings
WHERE id IN (:listing_ids);

-- Optional filter idiom (rare in MySQL code but present).
-- name: search_accounts :many
SELECT id, email
FROM accounts
WHERE (:status IS NULL OR status = :status)
  AND email LIKE :email_pattern
ORDER BY RAND()
LIMIT 10;

-- ------------------------------------------- aggregates, grouping, subqueries

-- name: account_offer_stats :one
SELECT
    COUNT(*) AS offer_count,
    COUNT(DISTINCT o.listingId) AS listing_count,
    SUM(IF(o.winning = 1, 1, 0)) AS winning_count,
    SUM(o.amount) AS total_amount,
    MAX(o.amount) AS max_amount,
    MIN(o.createdAt) AS first_offer_at,
    COALESCE(MAX(o.maxAmount), 0) AS max_ceiling
FROM offers o
WHERE o.accountId = :account_id;

-- GROUP BY with HAVING, DISTINCT, searched and simple CASE.
-- name: store_listing_summary :many
SELECT
    l.storeId AS store_id,
    CASE l.state WHEN 0 THEN 'draft' WHEN 1 THEN 'open' ELSE 'closed' END AS state_label,
    CASE WHEN SUM(l.currentPrice) > 1000 THEN 'high' ELSE 'low' END AS volume,
    COUNT(DISTINCT l.id) AS listings
FROM listings l
GROUP BY l.storeId, l.state
HAVING COUNT(*) > :min_listings
ORDER BY listings DESC;

-- JSON array built by string concatenation over GROUP_CONCAT, and JSON_ARRAYAGG
-- (MySQL 5.7.22+). JSON_ARRAYAGG over an empty LEFT JOIN yields [null], so the
-- empty case is selected explicitly.
-- name: pickup_listing_ids :one
SELECT
    p.id AS pickup_id,
    CONCAT('[', COALESCE(GROUP_CONCAT(DISTINCT pl.listingId), ''), ']') AS listing_ids_json,
    IF(COUNT(pl.listingId) = 0, JSON_ARRAY(), JSON_ARRAYAGG(pl.listingId)) AS listing_ids
FROM pickups p
LEFT JOIN pickup_listings pl ON pl.pickupId = p.id
WHERE p.id = :pickup_id
GROUP BY p.id;

-- Scalar subqueries in the select list and WHERE; EXISTS / NOT EXISTS; NOT IN.
-- name: list_listings_needing_pickup :many
SELECT
    l.id AS listing_id,
    (SELECT MAX(o.amount) FROM offers o WHERE o.listingId = l.id) AS high_offer,
    (SELECT o.accountId FROM offers o WHERE o.listingId = l.id AND o.winning = 1 LIMIT 1) AS winner_id
FROM listings l
WHERE l.closedAt IS NOT NULL
  AND EXISTS (SELECT 1 FROM offers o WHERE o.listingId = l.id AND o.winning = 1)
  AND NOT EXISTS (SELECT 1 FROM pickup_listings pl WHERE pl.listingId = l.id)
  AND l.storeId NOT IN (SELECT s.id FROM stores s WHERE s.active = 0)
  AND l.state NOT IN (3, 4);

-- Derived tables (nested), joined derived table, LIMIT defeating merge.
-- name: top_bidders_per_store :many
SELECT ranked.store_id, ranked.account_id, ranked.total
FROM (
    SELECT totals.store_id, totals.account_id, totals.total
    FROM (
        SELECT l.storeId AS store_id, o.accountId AS account_id, SUM(o.amount) AS total
        FROM offers o
        JOIN listings l ON l.id = o.listingId
        GROUP BY l.storeId, o.accountId
        LIMIT 18446744073709551615
    ) AS totals
    WHERE totals.total > :minimum
) AS ranked
LEFT JOIN (
    SELECT accountId, COUNT(*) AS missed
    FROM pickups
    WHERE missed = 1
    GROUP BY accountId
) AS misses ON misses.accountId = ranked.account_id
WHERE misses.missed IS NULL
ORDER BY ranked.total DESC;

-- UNION ALL across tables.
-- name: account_activity :many
SELECT 'offer' AS kind, o.id AS ref_id, o.createdAt AS happened_at
FROM offers o WHERE o.accountId = :account_id
UNION ALL
SELECT 'pickup' AS kind, p.id AS ref_id, p.scheduledFor AS happened_at
FROM pickups p WHERE p.accountId = :account_id
ORDER BY happened_at DESC
LIMIT :limit;

-- ROW_NUMBER emulation with user variables (no window functions on 5.7).
-- This mirrors the production shape; MySQL documents the evaluation order of
-- user-variable assignments as undefined, so the result is best-effort.
-- name: latest_offer_per_listing :many
SELECT ranked.listing_id, ranked.amount
FROM (
    SELECT
        o.listingId AS listing_id,
        o.amount AS amount,
        @rn := IF(@prev = o.listingId, @rn + 1, 1) AS rn,
        @prev := o.listingId AS prev_listing
    FROM offers o
    CROSS JOIN (SELECT @rn := 0, @prev := NULL) AS vars
    ORDER BY o.listingId, o.createdAt DESC
) AS ranked
WHERE ranked.rn = 1;

-- ---------------------------------------------------------- dates and times

-- Timezone conversion of a parameter and column, DATE(), DAYNAME, BETWEEN.
-- name: pickups_for_local_day :many
SELECT
    p.id AS pickup_id,
    DATE(CONVERT_TZ(p.scheduledFor, 'SYSTEM', s.timezone)) AS local_date,
    DAYNAME(CONVERT_TZ(p.scheduledFor, 'SYSTEM', s.timezone)) AS weekday,
    TIME(p.scheduledFor) AS scheduled_time
FROM pickups p
JOIN stores s ON s.id = p.storeId
WHERE DATE(CONVERT_TZ(p.scheduledFor, 'SYSTEM', s.timezone)) = DATE(CONVERT_TZ(:day, 'UTC', s.timezone))
  AND p.scheduledFor BETWEEN :window_start AND :window_end
  AND DAYNAME(CONVERT_TZ(p.scheduledFor, 'SYSTEM', s.timezone)) IN ('Saturday', 'Sunday');

-- Parameter inside CONCAT inside CONVERT_TZ.
-- name: count_pickups_on_date :one
SELECT COUNT(*) AS pickups
FROM pickups p
WHERE p.scheduledFor >= CONVERT_TZ(CONCAT(:day, 'T00:00:00'), :timezone, 'SYSTEM')
  AND p.scheduledFor < DATE_ADD(CONVERT_TZ(CONCAT(:day, 'T00:00:00'), :timezone, 'SYSTEM'), INTERVAL 1 DAY);

-- INTERVAL arithmetic with literal and parameter counts, DATE_ADD/DATE_SUB,
-- TIMESTAMPDIFF, UNIX_TIMESTAMP, FROM_UNIXTIME, DATE_FORMAT, STR_TO_DATE.
-- name: listing_timing :many
SELECT
    l.id AS listing_id,
    DATE_ADD(l.closesAt, INTERVAL :extend_minutes MINUTE) AS extended_close,
    DATE_SUB(NOW(), INTERVAL 7 DAY) AS week_ago,
    TIMESTAMPDIFF(MINUTE, l.createdAt, l.closesAt) AS open_minutes,
    UNIX_TIMESTAMP(l.closesAt) AS closes_epoch,
    FROM_UNIXTIME(0) AS epoch,
    DATE_FORMAT(l.closesAt, '%Y-%m-%d') AS close_day,
    STR_TO_DATE(:day, '%Y-%m-%d') AS parsed_day,
    TIMESTAMP(l.closesAt) AS close_ts,
    CURRENT_DATE AS today,
    CURRENT_TIMESTAMP(3) AS now_ms
FROM listings l
WHERE l.closesAt > NOW() - INTERVAL :hours HOUR
  AND l.closesAt < DATE_ADD(NOW(), INTERVAL 1 DAY);

-- Parameters inside IF/DATE_ADD in the select list, repeated parameter.
-- name: pickup_window :one
SELECT
    IF(:extended = 1, DATE_ADD(:day, INTERVAL 2 HOUR), :day) AS window_end,
    GREATEST(COALESCE(MAX(p.scheduledFor), :day), :day) AS latest
FROM pickups p
WHERE p.storeId = :store_id;

-- ----------------------------------------------------------------- JSON

-- name: account_preference :one
SELECT
    JSON_UNQUOTE(JSON_EXTRACT(a.preferences, '$[0].channel')) AS first_channel,
    NULLIF(a.firstName, '') AS first_name,
    LOWER(a.email) AS email,
    RIGHT(a.email, 4) AS email_suffix,
    ROUND(COALESCE(a.banned, 0)) AS banned_flag
FROM accounts a
WHERE a.id = :account_id;

-- ------------------------------------------------------------------ writes

-- INSERT with literal NOW(3), read back via lastrowid.
-- name: create_event :execlastid
INSERT INTO event_log (listingId, action, note, createdAt)
VALUES (:listing_id, :action, :note, NOW(3));

-- Batch insert through executemany.
-- name: create_offer :execmany
INSERT INTO offers (listingId, accountId, amount, maxAmount)
VALUES (:listing_id, :account_id, :amount, :max_amount);

-- INSERT IGNORE through executemany.
-- name: link_pickup_listing :execmany
INSERT IGNORE INTO pickup_listings (pickupId, listingId)
VALUES (?, ?);

-- Upsert forms: VALUES(col), parameter, increment, timestamp, no-op.
-- name: bump_counter :execrows
INSERT INTO account_counters (accountId, counterKey, hits, lastValue)
VALUES (:account_id, :counter_key, 1, :value)
ON DUPLICATE KEY UPDATE
    hits = hits + 1,
    lastValue = VALUES(lastValue),
    updatedAt = CURRENT_TIMESTAMP;

-- name: set_counter :execrows
INSERT INTO account_counters (accountId, counterKey, hits)
VALUES (:account_id, :counter_key, :hits)
ON DUPLICATE KEY UPDATE hits = :hits;

-- name: ensure_counter :exec
INSERT INTO account_counters (accountId, counterKey)
VALUES (:account_id, :counter_key)
ON DUPLICATE KEY UPDATE accountId = accountId;

-- Conditional upsert keeping the larger values.
-- name: record_offer_ceiling :execrows
INSERT INTO account_counters (accountId, counterKey, hits, lastValue)
VALUES (:account_id, 'ceiling', :hits, :value)
ON DUPLICATE KEY UPDATE
    hits = GREATEST(COALESCE(hits, 0), COALESCE(VALUES(hits), 0)),
    lastValue = IF(
        COALESCE(lastValue, 0) > COALESCE(VALUES(lastValue), 0),
        lastValue,
        VALUES(lastValue)
    );

-- Table alias, expression with the same parameter twice, optimistic guard.
-- name: close_listing :execrows
UPDATE listings l
SET
    l.state = IF(:outcome = 'sold', 2, 3),
    l.closedAt = NOW(),
    l.attributes = IF(:outcome = 'sold', l.attributes, NULL)
WHERE l.id = :listing_id
  AND l.state = :expected_state;

-- Update with list parameter and an index hint.
-- name: reopen_listings :execrows
UPDATE listings USE INDEX (PRIMARY)
SET state = 1, closedAt = NULL
WHERE id IN (:listing_ids) AND state = :old_state;

-- Update filtered through a joined subquery plus NOT EXISTS.
-- name: mark_missed_pickups :execrows
UPDATE pickups
SET missed = 1
WHERE storeId IN (
        SELECT s.id
        FROM stores s
        JOIN listings l ON l.storeId = s.id
        JOIN pickup_listings pl ON pl.listingId = l.id
        WHERE s.active = 1
    )
  AND scheduledFor < NOW() - INTERVAL 1 DAY
  AND arrivedAt IS NULL
  AND NOT EXISTS (
        SELECT 1
        FROM pickup_listings pl2
        JOIN event_log e ON e.listingId = pl2.listingId
        WHERE pl2.pickupId = pickups.id
    );

-- name: delete_pickup_links :execrows
DELETE FROM pickup_listings WHERE pickupId = :pickup_id AND listingId IN (:listing_ids);

-- Multi-table delete.
-- name: delete_events_for_store :execrows
DELETE e FROM event_log e
JOIN listings l ON l.id = e.listingId
WHERE l.storeId = ?;

-- Row-constructor anti-match.
-- name: prune_counters :execrows
DELETE FROM account_counters
WHERE accountId = :account_id
  AND (accountId, counterKey) NOT IN ((:account_id, 'ceiling'), (:account_id, 'visits'));

-- ------------------------------------------------------ locking and session

-- name: lock_listing :one
SELECT id, state, currentPrice AS current_price
FROM listings
WHERE id = :listing_id
FOR UPDATE;

-- name: set_isolation :exec
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

-- name: set_statement_timeout :exec
SET SESSION max_execution_time = 600000;

-- ---------------------------------------------------- dynamic SQL (open)
--
-- These are built in Python today; each needs a static design in butter
-- (or an explicit "not supported" answer):
--
-- 1. Optional WHERE fragments appended only when a filter is set
--    (`where += " AND col >= %(x)s"`), and lists of conditions joined with
--    " AND ". Static candidate: the `(:x IS NULL OR col >= :x)` idiom above.
-- 2. `LIMIT {n}` interpolated from an int. Static candidate: `LIMIT :n`.
-- 3. Placeholder lists generated with ",".join(["%s"] * n), including tuple
--    rows for `(a, b) NOT IN ((%s, %s), ...)`. Static candidate: a list
--    parameter (`IN (:ids)`) that the generated code expands.
-- 4. Column or table names interpolated from an enum or validated string
--    (`SET {column} = NULL`, `` FROM `{table}` ``).
-- 5. Conditional keywords (`... FOR UPDATE` only when locking).
-- 6. A SQL fragment returned by a helper and embedded as a derived table.
-- 7. `SET SESSION max_execution_time = {ms}` with an interpolated value.
