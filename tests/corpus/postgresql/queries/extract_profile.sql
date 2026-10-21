-- name: extract_profile :one
SELECT
    profile -> 'settings' AS settings_json,
    profile ->> 'timezone' AS timezone,
    profile #> '{settings,theme}' AS theme_json,
    profile #>> '{settings,theme}' AS theme_text,
    profile @> '{}'::jsonb AS contains_empty_object,
    '{}'::jsonb <@ profile AS contained_by_profile,
    jsonb_typeof(profile) AS profile_type,
    CASE
        WHEN jsonb_typeof(profile) = 'array' THEN jsonb_array_length(profile)
    END AS profile_array_length,
    jsonb_pretty(profile) AS profile_pretty,
    jsonb_typeof('null'::jsonb) AS json_type,
    jsonb_array_length('[]'::jsonb) AS array_length,
    jsonb_extract_path('{"settings":{}}'::jsonb, 'missing') AS missing_json_path,
    jsonb_extract_path_text('{"settings":{}}'::jsonb, 'missing') AS missing_text_path,
    jsonb_pretty('null'::jsonb) AS pretty_json
FROM public.accounts
WHERE email = $1;
