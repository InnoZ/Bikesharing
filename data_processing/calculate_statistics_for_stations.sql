--this script will combine observation periods and movements to gain average returns per station
--TODO: unify keys from 2015 and 2016

DROP TABLE IF EXISTS temp;

CREATE TABLE temp AS 
SELECT
*
FROM
(
  WITH station_periods AS (
  SELECT 
  *,
  CASE WHEN date_part('hour', seen_at) BETWEEN 0 AND 6 THEN departures ELSE 0 END AS "dep_0_6",
  CASE WHEN date_part('hour', seen_at) BETWEEN 6 AND 12 THEN departures ELSE 0 END AS "dep_6_12",
  CASE WHEN date_part('hour', seen_at) BETWEEN 12 AND 18 THEN departures ELSE 0 END AS "dep_12_18",
  CASE WHEN date_part('hour', seen_at) BETWEEN 18 AND 24 THEN departures ELSE 0 END AS "dep_18_24",
  CASE WHEN date_part('hour', seen_at) BETWEEN 0 AND 6 THEN returns ELSE 0 END AS "ret_0_6",
  CASE WHEN date_part('hour', seen_at) BETWEEN 6 AND 12 THEN returns ELSE 0 END AS "ret_6_12",
  CASE WHEN date_part('hour', seen_at) BETWEEN 12 AND 18 THEN returns ELSE 0 END AS "ret_12_18",
  CASE WHEN date_part('hour', seen_at) BETWEEN 18 AND 24 THEN returns ELSE 0 END AS "ret_18_24"
  FROM duesseldorf.station_movements
  )
  SELECT
  key,
  sum(departures) AS departures,
  sum(returns) AS returns,
  sum("dep_0_6") AS departures_0_6,
  sum("dep_6_12") AS departures_6_12,
  sum("dep_12_18") AS departures_12_18,
  sum("dep_18_24") AS departures_18_24,
  sum("ret_0_6") AS returns_0_6,
  sum("ret_6_12") AS returns_6_12,
  sum("ret_12_18") AS returns_12_18,
  sum("ret_18_24") AS returns_18_24
  FROM station_periods
  GROUP BY key
) t1
FULL OUTER JOIN 
(
  SELECT
  key AS key2,
  min(first_seen_at) AS first_seen_at,
  max(last_seen_at) AS last_seen_at,
  sum(CASE WHEN observation_type='observation' THEN duration ELSE NULL END) AS duration_observations,
  sum(CASE WHEN observation_type='empty_station' THEN duration ELSE NULL END) AS duration_empty_station,
  sum(CASE WHEN observation_type='uncertain' THEN duration ELSE NULL END) AS duration_uncertain
  FROM duesseldorf.station_observation_periods
  GROUP BY key
) t2
ON (t1.key = t2.key2)
;

-- calculate observed days
ALTER TABLE temp ADD COLUMN observed_days double precision;
UPDATE temp SET observed_days =
  EXTRACT(EPOCH FROM (duration_observations + duration_empty_station)/86400;

-- calculate average departures per day
ALTER TABLE temp ADD COLUMN avg_departures_per_day double precision;
UPDATE temp SET avg_departures_per_day =
  departures/observed_days;

-- calculate average returns per day
ALTER TABLE temp ADD COLUMN avg_returns_per_day double precision;
UPDATE temp SET avg_returns_per_day =
  returns/observed_days;


--combine station_statistics further with infos from duesseldorf.stations

DROP TABLE IF EXISTS duesseldorf.station_statistics;

CREATE TABLE duesseldorf.station_statistics AS 
SELECT
stats.*,
infos.provider,
infos.capacity,
infos.vehicle_type,
infos.latitude,
infos.longitude,
infos.geom
FROM
temp AS stats
LEFT JOIN 
duesseldorf.stations AS infos
ON stats.key = infos.key
;
