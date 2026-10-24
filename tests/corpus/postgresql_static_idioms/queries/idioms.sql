-- Optional filters. PostgreSQL needs a cast on a parameter tested with
-- IS NULL, because nothing else gives it a type there.
-- name: search_listings :many
-- param: store_ids list[int]
SELECT id, title, price
FROM listings
WHERE (:state::smallint IS NULL OR state = :state)
  AND (:min_price::float8 IS NULL OR price >= :min_price)
  AND store_id IN (:store_ids)
ORDER BY
    CASE WHEN :sort::text = 'price' THEN price END,
    id
LIMIT :limit;

-- Keyset pagination with an optional cursor.
-- name: page_listings :many
SELECT id FROM listings
WHERE (:before_id::bigint IS NULL OR id < :before_id)
ORDER BY id DESC
LIMIT :limit;

-- SET cannot take a bound parameter; set_config can (true = transaction-local).
-- name: set_lock_timeout :exec
SELECT set_config('lock_timeout', :timeout, true);
