-- Checked against PostgreSQL 16 through asyncpg: JSON values arrive as str,
-- arrays as lists, and aggregates over no rows as NULL.
-- name: summarize :many
SELECT
    owner,
    jsonb_build_object('owner', owner, 'count', count(*)) AS summary,
    jsonb_agg(id ORDER BY id) AS ids_json,
    json_object_agg(id, label) AS labels,
    array_agg(id ORDER BY id) AS ids,
    array_agg(label ORDER BY id) AS labels_array,
    array_agg(seen_at ORDER BY id) AS seen,
    array_agg(id * 2 ORDER BY id) AS doubled,
    to_jsonb(max(label)) AS last_label,
    COALESCE(jsonb_agg(meta) -> 0, '{}'::jsonb) || jsonb_build_object('n', count(*)) AS merged,
    jsonb_set('{}'::jsonb, '{k}', to_jsonb(count(*))) AS with_k,
    jsonb_build_array(1, 'a') AS arr
FROM items
GROUP BY owner
ORDER BY owner;

-- name: empty_agg :one
SELECT jsonb_agg(id) AS ids, array_agg(id) AS id_array FROM items WHERE id < 0;
