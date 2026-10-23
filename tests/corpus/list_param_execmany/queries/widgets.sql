-- name: delete_many :execmany
-- param: ids list[int]
DELETE FROM widgets WHERE id IN (:ids);
