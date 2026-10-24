-- Widget queries
--
-- A bare `--` line is a comment in MySQL.

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

-- List parameters expand at call time; an empty list matches nothing for IN
-- and everything for NOT IN.
-- name: widgets_by_ids :many
-- param: ids list[int]
-- param: excluded list[str]
SELECT id, name FROM widgets WHERE id IN (:ids) AND name NOT IN ( :excluded ) ORDER BY id;
