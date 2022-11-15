\set periods_first_day '2021-01-01'
\set periods_last_day '2021-02-28'
\set min_allowable_2_month_parking_time '5 hours'
\set min_unique_days 15
\set selected_date '2021-04-23'

EXPLAIN ANALYSE WITH date_filtered AS (
  SELECT
    accountid,
    zone_number,
    CASE
      WHEN start_time::date < :'periods_first_day'::date
      THEN :'periods_first_day'::timestamp
      ELSE start_time
    END AS start_time,
    CASE
      WHEN end_time::date > :'periods_last_day'::date
      THEN :'periods_last_day'::timestamp + '23:59:59.999'::interval
      ELSE end_time
    END AS end_time
  FROM
    parking_session
  WHERE
    start_time::date <= :'periods_last_day'::date AND
    end_time::date >= :'periods_first_day'::date
),
favourite AS (
  SELECT
    accountid,
    zone_number,
    SUM(end_time - start_time) AS parking_time,
    MAX(SUM(end_time - start_time)) OVER (PARTITION BY accountid) AS max_parking_time
  FROM
    date_filtered
  GROUP BY
    accountid,
    zone_number
  HAVING
    SUM(end_time - start_time) > :'min_allowable_2_month_parking_time'::interval
),
regular AS (
  SELECT
    accountid,
    zone_number
  FROM
    generate_series(
      :'periods_first_day'::timestamp,
      :'periods_last_day'::timestamp,
      '1 day'::interval
    ) AS day_of_period
    INNER JOIN date_filtered ON day_of_period::date BETWEEN start_time::date AND end_time::date
    INNER JOIN favourite USING(accountid, zone_number)
  WHERE parking_time = max_parking_time
  GROUP BY
    accountid,
    zone_number
  HAVING
    COUNT(
      DISTINCT day_of_period::date
    ) > :'min_unique_days'
),
selected_day AS (
  SELECT
    EXTRACT(HOUR FROM selected_hour) + 1 AS hour,
    zone_number,
    CASE
      WHEN (accountid, zone_number) IN (SELECT accountid, zone_number FROM regular)
      THEN 1
      ELSE 0 
    END AS is_regular,
    CASE
      WHEN end_time > selected_hour + '1 hour'::interval
      THEN selected_hour + '1 hour'::interval
      ELSE end_time
    END - CASE 
      WHEN start_time < selected_hour
      THEN selected_hour
      ELSE start_time
    END AS parking_time
  FROM
    generate_series(
      :'selected_date'::date,
      :'selected_date'::date + '23:00:00'::interval,
      '1 hour'::interval
    ) AS selected_hour
    INNER JOIN parking_session ON
      :'selected_date'::date BETWEEN start_time::date AND end_time::date AND
      start_time < selected_hour + '1 hour'::interval AND
      end_time > selected_hour
)
SELECT
  hour,
  zone_number,
  SUM(parking_time * is_regular) AS regulars_time,
  SUM(parking_time) AS all_time,
  ROUND(
    EXTRACT(EPOCH FROM SUM(parking_time * is_regular)) / 
    EXTRACT(EPOCH FROM SUM(parking_time)) * 100, 2
  ) AS percentage_of_regulars
FROM
  selected_day
GROUP BY
  hour,
  zone_number
ORDER BY
  hour,
  zone_number
