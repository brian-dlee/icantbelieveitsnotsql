-- name: list_account_activity :many
WITH recent AS (
    SELECT account_id, amount, memo
    FROM invoice
    WHERE amount >= :minimum
)
SELECT a.id, a.display_name, r.amount, r.memo
FROM account AS a
LEFT JOIN recent AS r ON r.account_id = a.id
UNION ALL
SELECT a.id, a.display_name, NULL AS amount, 'no invoice' AS memo
FROM account AS a
WHERE NOT EXISTS (SELECT 1 FROM invoice AS i WHERE i.account_id = a.id)
ORDER BY id;
