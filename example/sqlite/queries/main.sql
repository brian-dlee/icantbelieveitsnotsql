-- SELECT
SELECT id, email, created_at
FROM users
WHERE id = ?;

SELECT o.order_id, o.status, oi.product_id, oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = ?;

-- INSERT
INSERT INTO users (email, created_at)
VALUES (?, datetime('now'));

INSERT INTO order_items (order_id, product_id, quantity)
VALUES (?, ?, ?);

-- UPDATE
UPDATE users
SET email = ?
WHERE id = ?;

UPDATE accounts
SET balance = balance + ?
WHERE account_id = ?;

-- DELETE
DELETE FROM users
WHERE id = ?;

DELETE FROM order_items
WHERE order_id = ? AND product_id = ?;
