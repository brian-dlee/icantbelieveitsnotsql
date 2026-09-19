-- name: create_table_membership :exec
CREATE TABLE IF NOT EXISTS membership (
    user_id TEXT NOT NULL,
    group_id TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_membership_user_id_group_id ON membership (user_id, group_id);

-- name: insert_many_membership :execmany
INSERT INTO membership (user_id, group_id)
VALUES (:user_id, :group_id);

-- name: insert_many_membership_ignore_conflicts :execmany
INSERT INTO membership (user_id, group_id)
VALUES (:user_id, :group_id)
ON CONFLICT(user_id, group_id) DO NOTHING;

-- name: select_many_membership_by_user_id :many
SELECT *
FROM membership
WHERE user_id = :user_id;

-- name: select_many_membership_with_cache_entry :many
-- A LEFT JOIN makes the joined columns nullable.
SELECT m.user_id, m.group_id, c.value AS cached_value
FROM membership m
LEFT JOIN cache_entry c ON c.key = m.group_id
WHERE m.user_id = :user_id;
