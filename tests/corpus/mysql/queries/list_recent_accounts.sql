-- name: list_recent_accounts :many
SELECT id, email
FROM accounts
WHERE created_at > NOW() - INTERVAL ? DAY
  AND status = ?;
