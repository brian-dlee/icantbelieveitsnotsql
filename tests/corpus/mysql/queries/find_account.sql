-- name: find_account :one
SELECT id, email, profile, status, balance
FROM accounts
WHERE email = ?
LIMIT ?;
