-- name: get_user_by_id :one
SELECT id AS user_id, email, created_at
FROM users
WHERE id = ?;

-- name: list_order_items_for_customer :many
SELECT o.order_id, o.status, oi.product_id, oi.quantity
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.customer_id = ?;

-- name: create_user :execlastid
INSERT INTO users (email, created_at)
VALUES (?, datetime('now'));

-- name: add_order_items :execmany
INSERT INTO order_items (order_id, product_id, quantity)
VALUES (:order_id, :product_id, :quantity);

-- name: update_user_email :execrows
UPDATE users
SET email = :email
WHERE id = :id;

-- name: adjust_balance :execrows
UPDATE accounts
SET balance = balance + :amount
WHERE account_id = :account_id;

-- name: delete_user :execrows
DELETE FROM users
WHERE id = ?;

-- name: delete_order_item :execrows
DELETE FROM order_items
WHERE order_id = ? AND product_id = ?;

-- name: rectangle_areas :many
SELECT id, width, height, area
FROM rectangles
WHERE area > :min_area;
