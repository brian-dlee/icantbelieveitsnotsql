-- Each column was checked against MySQL 5.7 through PyMySQL: the inferred type
-- matches what the driver returns, or what pydantic converts it to losslessly.
-- name: probe :one
SELECT
    DATE(e.at0) AS d,
    TIME(e.at0) AS t,
    e.slot AS slot,
    TIMESTAMP(e.day) AS ts,
    DATE_ADD(e.day, INTERVAL 1 DAY) AS day_plus_day,
    DATE_ADD(e.day, INTERVAL 1 HOUR) AS day_plus_hour,
    DATE_SUB(e.at0, INTERVAL 7 DAY) AS week_ago,
    DATE_ADD(:start, INTERVAL 2 HOUR) AS param_plus_hour,
    STR_TO_DATE('2024-01-02', '%Y-%m-%d') AS parsed_day,
    STR_TO_DATE('2024-01-02 03:04', '%Y-%m-%d %H:%i') AS parsed_ts,
    UNIX_TIMESTAMP(e.at0) AS epoch0,
    UNIX_TIMESTAMP(e.at3) AS epoch3,
    UNIX_TIMESTAMP(e.day) AS epoch_day,
    UNIX_TIMESTAMP(:moment) AS epoch_param,
    FROM_UNIXTIME(-1) AS before_epoch,
    TIMESTAMPADD(MINUTE, :minutes, e.at0) AS later_by,
    FROM_UNIXTIME(0) AS epoch_ts,
    TIMESTAMPDIFF(MINUTE, e.at0, e.at3) AS minutes,
    CONVERT_TZ(e.at0, '+00:00', '+02:00') AS shifted,
    CONVERT_TZ(e.at0, 'UTC', e.tz) AS named_shift,
    DAYNAME(e.at0) AS weekday,
    DAYOFWEEK(e.at0) AS weekday_number,
    DATE_FORMAT(e.at0, '%Y-%m') AS ym,
    GREATEST(e.at0, e.at3) AS later,
    LEAST(e.day, :cutoff) AS earlier,
    RAND() AS r,
    RIGHT(e.tz, 3) AS tz_tail,
    JSON_UNQUOTE(JSON_EXTRACT(e.payload, '$.k')) AS k,
    JSON_EXTRACT(e.payload, '$.n') AS n_json,
    JSON_ARRAY(1, 2) AS arr,
    CURDATE() AS today,
    CURTIME() AS now_time
FROM events e
WHERE e.id = :id;

-- name: agg :one
SELECT JSON_ARRAYAGG(e.id) AS ids FROM events e;
