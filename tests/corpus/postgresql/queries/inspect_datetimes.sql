-- name: inspect_datetimes :many
SELECT
    now() AS checked_at,
    current_date AS checked_on,
    date_trunc('day', created_at) AS created_day,
    date_trunc('hour', trial_length) AS trial_hours,
    EXTRACT(YEAR FROM created_at) AS created_year,
    date_part('dow', created_at) AS created_weekday,
    age(now(), created_at) AS account_age,
    to_char(created_at, 'YYYY-MM-DD') AS created_label,
    created_at + interval '30 days' AS trial_ends_at,
    created_at + trial_length AS trial_ended_at,
    created_at::date - DATE '2024-01-01' AS days_since_launch,
    created_at::date + 7 AS week_later,
    now() - created_at AS elapsed,
    trial_length * 2 AS doubled_trial,
    -trial_length AS negated_trial,
    TIMESTAMP '2024-01-01 00:00:00' AS launched_at,
    make_date(2024, 1, 1) AS launch_date
FROM public.accounts
WHERE created_at >= date_trunc('day', $1::timestamptz)
  AND created_at < now() - interval '1 hour';
