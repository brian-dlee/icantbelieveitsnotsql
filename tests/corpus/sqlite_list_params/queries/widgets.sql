-- name: widgets_by_ids :many
-- param: ids list[int]
SELECT id, name FROM widgets WHERE id IN (:ids) AND price >= :min_price ORDER BY id;

-- The same list in two statements expands in each.
-- name: delete_widgets :execrows
-- param: ids list[int]
DELETE FROM widgets WHERE id NOT IN (:ids);
