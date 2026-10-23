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
