-- ROW_NUMBER emulation with user variables (MySQL 5.7 has no window functions),
-- checked against MySQL 5.7 through PyMySQL.
-- name: latest_offer_per_listing :many
SELECT ranked.listing_id, ranked.amount, ranked.rn
FROM (
    SELECT
        o.listingId AS listing_id,
        o.amount AS amount,
        @rn := IF(@prev = o.listingId, @rn + 1, 1) AS rn,
        @prev := o.listingId AS prev_listing
    FROM offers o
    CROSS JOIN (SELECT @rn := 0, @prev := NULL) AS vars
    ORDER BY o.listingId, o.createdAt DESC
) AS ranked
WHERE ranked.rn <= :top;

-- name: counters :one
SELECT @total := COUNT(*), @@session.sql_mode AS sql_mode FROM offers;
