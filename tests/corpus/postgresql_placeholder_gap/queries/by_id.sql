-- name: by_id :one
SELECT id FROM accounts WHERE id = $2;
