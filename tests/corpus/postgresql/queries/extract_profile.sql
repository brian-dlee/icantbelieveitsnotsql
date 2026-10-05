-- name: extract_profile :one
SELECT
    profile -> 'settings' AS settings_json,
    profile ->> 'timezone' AS timezone,
    profile #> '{settings,theme}' AS theme_json,
    profile #>> '{settings,theme}' AS theme_text
FROM public.accounts
WHERE email = $1;
