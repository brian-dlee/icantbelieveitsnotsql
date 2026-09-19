-- name: create_table_tag :exec
CREATE TABLE IF NOT EXISTS tag (
    item_id TEXT NOT NULL,
    name TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tag_item_id_name ON tag (item_id, name);

-- name: insert_many_tag :execmany
INSERT INTO tag (item_id, name)
VALUES (:item_id, :name)
ON CONFLICT(item_id, name) DO NOTHING;

-- name: select_many_tag_name_by_item_id :many
SELECT name
FROM tag
WHERE item_id = :item_id
ORDER BY name;
