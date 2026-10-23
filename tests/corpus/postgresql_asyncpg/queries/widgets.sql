-- Named placeholders are numbered in order of first appearance.
-- name: create_widget :one
INSERT INTO widgets (name, price, meta) VALUES (:name, :price, :meta::jsonb)
RETURNING id;

-- name: create_widgets :execmany
INSERT INTO widgets (name, price) VALUES ($1, $2);

-- name: reprice :execrows
UPDATE widgets SET price = price * 2 WHERE name LIKE '%gadget%' AND price < :limit;

-- name: list_widgets :many
SELECT id, name FROM widgets ORDER BY id;

-- Several statements share one session, so this takes a Connection, not a Pool.
-- name: reset_widgets :exec
SET lock_timeout = '5s';
DELETE FROM widgets;

-- Casts are a single `::` token, so a parameter named like the cast type
-- (`text` here) never captures the type name.
-- name: find_by_label :many
SELECT id, 1::text AS one_text FROM widgets WHERE name = :text AND meta = :meta::jsonb;

-- Optional filters: a cast or parentheses around the placeholder still make it nullable.
-- name: search_widgets :many
SELECT id FROM widgets
WHERE ($1::text IS NULL OR name = $1)
  AND (($2::bigint) IS NULL OR id < $2)
ORDER BY id DESC
LIMIT $3;
