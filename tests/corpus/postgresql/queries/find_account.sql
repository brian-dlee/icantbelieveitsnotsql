-- name: find_account :one
SELECT id, external_id, email, profile, tags, score_matrix
FROM public.accounts
WHERE id > $2 AND (email = $1 OR lower(email) = lower($1)) AND tags = $3;
