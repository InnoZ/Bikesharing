--calculate geometries, attributes of leading entires and idle times for sightings
DROP TABLE IF EXISTS bikesharing.temp1;
CREATE TABLE bikesharing.temp1 AS
SELECT
	*,
	last_seen_at - first_seen_at AS idle_time,
	lead(first_seen_at) OVER (ORDER BY key, first_seen_at) AS lead_first_seen_at,
	lead(key) OVER (ORDER BY key, first_seen_at) AS lead_key,
	lead(fuel_level) OVER (ORDER BY key, first_seen_at) AS lead_fuel_level,
	st_setsrid(ST_Point(
		lead(longitude) OVER (ORDER BY key, first_seen_at),
		lead(latitude) OVER (ORDER BY key, first_seen_at)), 4326)
		AS lead_geom,
	st_setsrid(ST_Point(longitude, latitude), 4326) AS geom
FROM bikesharing.vehicle_sightings;

-- label last sightings of a specific car
ALTER TABLE bikesharing.temp1 ADD COLUMN last_sighting boolean;
UPDATE bikesharing.temp1 SET last_sighting = CASE WHEN key <> lead_key THEN TRUE ELSE FALSE END;

-- calculate duration, crow fly distance of outgoing trips
DROP TABLE IF EXISTS bikesharing.vehicle_sightings_extended;
CREATE TABLE bikesharing.vehicle_sightings_extended AS
SELECT
	*,
	(CASE WHEN last_sighting IS FALSE THEN lead_first_seen_at - last_seen_at ELSE NULL END) AS duration,
	(CASE WHEN last_sighting IS FALSE THEN  st_distance_sphere(geom, lead_geom) ELSE NULL END) AS crowflydistance,
	(CASE WHEN last_sighting IS FALSE THEN  lead_fuel_level - fuel_level ELSE NULL END) AS fuel_level_difference,
	(CASE WHEN last_sighting IS FALSE THEN  st_setsrid(st_makeline(geom, lead_geom), 4326) ELSE NULL END) AS line_geom
FROM bikesharing.temp1;

-- delete rows with unlogical values
DELETE FROM bikesharing.vehicle_sightings_extended
  WHERE crowflydistance < 0 OR idle_time < '00:00:00' OR duration < '00:00:00';

-- update times to match booking data
-- TODO: check with timezones etc. to identify source of problem
UPDATE bikesharing.vehicle_sightings_extended
  SET first_seen_at = first_seen_at + interval '04:00:00';
UPDATE bikesharing.vehicle_sightings_extended
  SET last_seen_at = last_seen_at + interval '04:00:00';
UPDATE bikesharing.vehicle_sightings_extended
  SET lead_first_seen_at = lead_first_seen_at + interval '04:00:00';

-- index: bike_sightings_extended_provider_idx

DROP INDEX IF EXISTS bike_sightings_extended_provider_idx;

CREATE INDEX bike_sightings_extended_provider_idx
  ON bikesharing.bike_sightings_extended
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: bike_sightings_extended_stationary_idx

DROP INDEX IF EXISTS bike_sightings_extended_stationary_idx;

CREATE INDEX bike_sightings_extended_stationary_idx
  ON bikesharing.bike_sightings_extended
  USING btree
  (stationary);

-- index: bike_sightings_extended_bike_type_idx

DROP INDEX IF EXISTS bike_sightings_extended_bike_type_idx;

CREATE INDEX bike_sightings_extended_bike_type_idx
  ON bikesharing.bike_sightings_extended
  USING btree
  (bike_type COLLATE pg_catalog."default");

-- index: bike_sightings_extended_geom_gist

DROP INDEX IF EXISTS bike_sightings_extended_geom_gist;

CREATE INDEX bike_sightings_extended_geom_gist
  ON bikesharing.bike_sightings_extended
  USING gist
  (geom);

-- index: bike_sightings_extended_lead_geom_gist

DROP INDEX IF EXISTS bike_sightings_extended_lead_geom_gist;

CREATE INDEX bike_sightings_extended_lead_geom_gist
  ON bikesharing.bike_sightings_extended
  USING gist
  (lead_geom);

-- index: bike_sightings_extended_line_geom_gist

DROP INDEX IF EXISTS bike_sightings_extended_line_geom_gist;

CREATE INDEX bike_sightings_extended_line_geom_gist
  ON bikesharing.bike_sightings_extended
  USING gist
  (line_geom);
