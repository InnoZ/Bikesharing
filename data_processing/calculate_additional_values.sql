--calculate geometries, attributes of leading entires and idle times for movements
DROP TABLE IF EXISTS bikesharing.temp1;
CREATE TABLE bikesharing.temp1 AS
SELECT
	*,
	ended_at - started_at AS duration,
	lead(started_at) OVER (ORDER BY key, started_at) AS lead_started_at,
	lead(key) OVER (ORDER BY key, started_at) AS lead_key,
	st_setsrid(ST_Point(longitude_start, latitude_start), 4326) AS geom_start,
	st_setsrid(ST_Point(longitude_end, latitude_end), 4326) AS geom_end
FROM bikesharing.vehicle_movements
WHERE city='berlin' AND started_at BETWEEN '2016-09-01 00:00' AND '2016-10-01 00:00';

-- label last movement of a specific vehicle
ALTER TABLE bikesharing.temp1 ADD COLUMN last_movement boolean;
UPDATE bikesharing.temp1 SET last_movement = CASE WHEN key <> lead_key THEN TRUE ELSE FALSE END;

-- calculate duration, crow fly distance of outgoing trips
DROP TABLE IF EXISTS bikesharing.vehicle_movements_extended;
CREATE TABLE bikesharing.vehicle_movements_extended AS
SELECT
	*,
	(CASE WHEN last_movement IS FALSE THEN lead_started_at - ended_at ELSE NULL END) AS lead_idle_time,
	(CASE WHEN last_movement IS FALSE THEN  st_distance_sphere(geom_start, geom_end) ELSE NULL END) AS crowflydistance
FROM bikesharing.temp1;

-- delete rows with unlogical values
DELETE FROM bikesharing.vehicle_movements_extended
  WHERE crowflydistance < 0 OR lead_idle_time < '00:00:00' OR duration < '00:00:00';

-- -- update times to match booking data
-- -- TODO: check with timezones etc. to identify source of problem
-- UPDATE bikesharing.vehicle_movements_extended
--   SET started_at = started_at + interval '04:00:00';
-- UPDATE bikesharing.vehicle_movements_extended
--   SET ended_at = ended_at + interval '04:00:00';
-- UPDATE bikesharing.vehicle_movements_extended
--   SET lead_started_at = lead_started_at + interval '04:00:00';

-- index: vehicle_movements_extended_provider_idx
DROP INDEX IF EXISTS vehicle_movements_extended_provider_idx;
CREATE INDEX vehicle_movements_extended_provider_idx
  ON bikesharing.vehicle_movements_extended
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: vehicle_movements_extended_stationary_idx
DROP INDEX IF EXISTS vehicle_movements_extended_stationary_idx;
CREATE INDEX vehicle_movements_extended_stationary_idx
  ON bikesharing.vehicle_movements_extended
  USING btree
  (stationary);

-- index: vehicle_movements_extended_lead_geom_gist
DROP INDEX IF EXISTS vehicle_movements_extended_geom_start_gist;
CREATE INDEX vehicle_movements_extended_geom_start_gist
  ON bikesharing.vehicle_movements_extended
  USING gist
  (geom_start);

-- index: vehicle_movements_extended_line_geom_gist
DROP INDEX IF EXISTS vehicle_movements_extended_geom_end_gist;
CREATE INDEX vehicle_movements_extended_geom_end_gist
  ON bikesharing.vehicle_movements_extended
  USING gist
  (geom_end);
