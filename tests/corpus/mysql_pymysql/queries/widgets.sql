-- name: create_widget :execlastid
INSERT INTO widgets (name, price) VALUES (:name, :price);

-- name: create_widgets :execmany
INSERT INTO widgets (name, price) VALUES (?, ?);

-- Literal `%` must be doubled because parameters are bound.
-- name: find_gadgets :many
SELECT id, name AS widget_name
FROM widgets
WHERE name LIKE '%gadget%' AND (price >= :min_price OR price = :min_price)
ORDER BY id
LIMIT :limit;

-- No parameters: the driver skips interpolation, so `%` stays as written.
-- name: count_with_a :one
SELECT COUNT(*) AS n FROM widgets WHERE name LIKE '%a%';
