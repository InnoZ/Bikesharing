--this script will extract observation intervals for
--bikesharing stations

--calculate temporary table to get leading and lagging infos
DROP TABLE IF EXISTS bikesharing.temp;
DROP TABLE IF EXISTS bikesharing.station_observation_periods;

CREATE TABLE bikesharing.temp AS
SELECT
key,
seen_at,
vehicles_available,
lead(key) OVER (ORDER BY key, seen_at) AS lead_key,
lag(key) OVER (ORDER BY key, seen_at) AS lag_key,
lag(vehicles_available) OVER (ORDER BY key, seen_at) AS lag_vehicles_available
FROM bikesharing.station_movements
WHERE provider IN
  (
    'nextbike-berlin',
    'nextbike-frankfurt',
    'nextbike-hamburg',
    'nextbike germany',
    'kvb-rad-koln',
    'kvb-rad',
    'kvb rad germany',
    'mvgrad',
    'nextbike-munchen'
  )
;

--label beginning and end of an observation interval
ALTER TABLE bikesharing.temp ADD COLUMN observation_type varchar;
UPDATE bikesharing.temp SET observation_type  =
  CASE WHEN key <> lead_key
    THEN 'end'
    ELSE NULL
  END;
UPDATE bikesharing.temp SET lag_vehicles_available  =
  CASE WHEN key <> lag_key
    THEN NULL
    ELSE lag_vehicles_available
  END;
UPDATE bikesharing.temp SET observation_type =
  CASE WHEN key <> lag_key AND vehicles_available BETWEEN 1 AND 4
    THEN 'observation'
  WHEN key <> lag_key AND vehicles_available = 0
    THEN 'empty_station'
  WHEN key <> lag_key AND vehicles_available = 5
    THEN 'uncertain'
    ELSE observation_type
  END;

--label beginning
UPDATE bikesharing.temp SET observation_type =
  CASE WHEN key = lag_key AND (lag_vehicles_available = 5 OR lag_vehicles_available = 0) AND vehicles_available BETWEEN 1 AND 4
    THEN 'observation'
  WHEN key = lag_key AND lag_vehicles_available != 0 AND vehicles_available = 0
    THEN 'empty_station'
  WHEN key = lag_key AND lag_vehicles_available != 5 AND vehicles_available = 5
    THEN 'uncertain'
    ELSE observation_type
  END;

--delete rows with no observation period information
DELETE FROM bikesharing.temp WHERE observation_type IS NULL;

CREATE TABLE bikesharing.station_observation_periods AS
SELECT
key,
seen_at AS first_seen_at,
lead(seen_at) OVER (ORDER BY key, seen_at) AS last_seen_at,
lead(seen_at) OVER (ORDER BY key, seen_at) - seen_at AS duration,
observation_type
FROM bikesharing.temp;

DROP TABLE bikesharing.temp;

--set last_seen_at for observations
UPDATE bikesharing.station_observation_periods SET last_seen_at =
  CASE WHEN observation_type != 'end'
    THEN last_seen_at
    ELSE NULL
  END;

--extract winter break by removing last_seen_at and duration
--TODO: set last_seen_at as start of winter break
UPDATE bikesharing.station_observation_periods SET last_seen_at =
  CASE WHEN date_part('year', first_seen_at) = date_part('year', last_seen_at)
    THEN last_seen_at
    ELSE NULL
  END;

UPDATE bikesharing.station_observation_periods SET duration =
  CASE WHEN date_part('year', first_seen_at) = date_part('year', last_seen_at)
    THEN duration
    ELSE NULL
  END;
