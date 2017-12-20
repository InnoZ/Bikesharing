-- this bash will transform sightings to movements for bikesharing data
-- TODO: not null for first_seen_at, last_seen_at and location
INSERT INTO bikesharing.vehicle_movements
	(
	  provider,
	  city,
	  key,
	  started_at,
	  ended_at,
	  start_station_id,
	  start_station_name,
	  latitude_start,
	  longitude_start,
	  end_station_id,
	  end_station_name,
	  latitude_end,
	  longitude_end,
	  stationary,
	  price,
	  vehicle_type,
	  from_movements
	)
SELECT
	provider,
	city,
	key,
	-- end of previous sighting (last_seen_at) as start of movement (started_at)
	lag(last_seen_at) OVER (ORDER BY key, first_seen_at),
	-- start from sighting (first_seen_at) as end of movement (ended_at)
	first_seen_at AS ended_at,
	NULL AS start_station_id,
	NULL AS start_station_name,
	lag(latitude) OVER (ORDER BY key, first_seen_at) AS latitude_start,
	lag(longitude) OVER (ORDER BY key, first_seen_at) AS longitude_start,
	NULL AS end_station_id,
	NULL AS end_station_name,
	latitude AS latitude_end,
	longitude AS longitude_end,
	stationary AS stationary,
	NULL AS price,
	vehicle_type,
	FALSE AS from_movements
FROM bikesharing.vehicle_sightings
;
