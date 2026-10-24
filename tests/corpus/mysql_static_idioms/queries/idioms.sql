-- Optional filters: a NULL argument disables its condition, replacing
-- `where += " AND ..."` string building.
-- name: search_listings :many
-- param: store_ids list[int]
SELECT id, title, price
FROM listings
WHERE (:state IS NULL OR state = :state)
  AND (:min_price IS NULL OR price >= :min_price)
  AND (:closes_before IS NULL OR closesAt < :closes_before)
  AND storeId IN (:store_ids)
ORDER BY
    CASE WHEN :sort = 'price' THEN price END,
    CASE WHEN :sort = 'closing' THEN closesAt END,
    id
LIMIT :limit;

-- Conditional locking: one named query per variant instead of
-- `{"FOR UPDATE" if lock else ""}`.
-- name: get_listing :one
SELECT id, state FROM listings WHERE id = :id;

-- name: get_listing_for_update :one
SELECT id, state FROM listings WHERE id = :id FOR UPDATE;

-- Session settings take a parameter; PyMySQL substitutes it client-side.
-- name: set_statement_timeout :exec
-- param: milliseconds int
SET SESSION max_execution_time = :milliseconds;
