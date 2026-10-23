-- name: by_ids :many
-- param: id list[int]
SELECT id FROM widgets WHERE id IN (?);
