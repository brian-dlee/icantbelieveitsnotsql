-- name: by_ids :many
-- param: ids list[int]
SELECT id FROM widgets WHERE id = :ids;
