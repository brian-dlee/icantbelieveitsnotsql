-- get_user_by_id :one
SELECT id AS user_id, email, created_at
FROM users
WHERE id = :id;

-- get_orders_with_items :many
SELECT o.order_id, o.status, oi.product_id, oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = :customer_id;

-- create_user :exec
INSERT INTO users (email, created_at)
VALUES (:email, datetime('now'));

-- create_order_item :exec
INSERT INTO order_items (order_id, product_id, quantity)
VALUES (:order_id, :product_id, :quantity);

-- update_user_email :exec
UPDATE users
SET email = :email
WHERE id = :id;

-- adjust_account_balance :exec
UPDATE accounts
SET balance = balance + :amount
WHERE account_id = :account_id;

-- delete_user :exec
DELETE FROM users
WHERE id = :id;

-- delete_order_item :exec
DELETE FROM order_items
WHERE order_id = :order_id AND product_id = :product_id;
