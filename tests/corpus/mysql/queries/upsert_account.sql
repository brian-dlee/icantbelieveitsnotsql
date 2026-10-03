-- name: upsert_account :execrows
INSERT INTO accounts (email, status, balance)
VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE balance = VALUES(balance);
