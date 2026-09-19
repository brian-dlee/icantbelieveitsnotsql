-- name: create_table_cache_entry :exec
CREATE TABLE IF NOT EXISTS cache_entry (
    key TEXT NOT NULL PRIMARY KEY UNIQUE,
    value BLOB NOT NULL
);

-- name: select_many_cache_entry :many
SELECT *
FROM cache_entry;

-- name: select_many_cache_entry_by_key :many
SELECT *
FROM cache_entry
WHERE key = :key;

-- name: select_one_cache_entry_by_key :one
-- A `?` placeholder is named after the column it is compared with.
SELECT *
FROM cache_entry
WHERE key = ?;

-- name: insert_many_cache_entry :execmany
INSERT INTO cache_entry (key, value)
VALUES (:key, :value);

-- name: count_cache_entry :one
SELECT COUNT(*) AS total
FROM cache_entry;
