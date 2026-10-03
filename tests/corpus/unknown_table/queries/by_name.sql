-- name: by_name :one
SELECT id FROM acount WHERE display_name = :name;
