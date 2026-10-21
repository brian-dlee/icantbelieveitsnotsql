-- name: extract_profile :one
SELECT
    profile -> 'settings' AS settings_json,
    profile ->> 'timezone' AS timezone,
    profile #> '{settings,theme}' AS theme_json,
    profile #>> '{settings,theme}' AS theme_text,
    profile @> '{}'::jsonb AS contains_empty_object,
    '{}'::jsonb <@ profile AS contained_by_profile
FROM public.accounts
WHERE email = $1;
