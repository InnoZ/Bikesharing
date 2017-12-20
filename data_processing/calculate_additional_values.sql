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
FROM bikesharing.vehicle_movements;

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
  WHERE crowflydistance < 0 OR idle_time < '00:00:00' OR duration < '00:00:00';

-- -- update times to match booking data
-- -- TODO: check with timezones etc. to identify source of problem
-- UPDATE bikesharing.vehicle_movements_extended
--   SET started_at = started_at + interval '04:00:00';
-- UPDATE bikesharing.vehicle_movements_extended
--   SET ended_at = ended_at + interval '04:00:00';
-- UPDATE bikesharing.vehicle_movements_extended
--   SET lead_started_at = lead_started_at + interval '04:00:00';

-- index: bike_movements_extended_provider_idx

DROP INDEX IF EXISTS bike_movements_extended_provider_idx;

CREATE INDEX bike_movements_extended_provider_idx
  ON bikesharing.bike_movements_extended
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: bike_movements_extended_stationary_idx

DROP INDEX IF EXISTS bike_movements_extended_stationary_idx;

CREATE INDEX bike_movements_extended_stationary_idx
  ON bikesharing.bike_movements_extended
  USING btree
  (stationary);

-- index: bike_movements_extended_bike_type_idx

DROP INDEX IF EXISTS bike_movements_extended_bike_type_idx;

CREATE INDEX bike_movements_extended_bike_type_idx
  ON bikesharing.bike_movements_extended
  USING btree
  (bike_type COLLATE pg_catalog."default");

-- index: bike_movements_extended_geom_gist

DROP INDEX IF EXISTS bike_movements_extended_geom_gist;

CREATE INDEX bike_movements_extended_geom_gist
  ON bikesharing.bike_movements_extended
  USING gist
  (geom);

-- index: bike_movements_extended_lead_geom_gist

DROP INDEX IF EXISTS bike_movements_extended_lead_geom_gist;

CREATE INDEX bike_movements_extended_lead_geom_gist
  ON bikesharing.bike_movements_extended
  USING gist
  (lead_geom);

-- index: bike_movements_extended_line_geom_gist

DROP INDEX IF EXISTS bike_movements_extended_line_geom_gist;

CREATE INDEX bike_movements_extended_line_geom_gist
  ON bikesharing.bike_movements_extended
  USING gist
  (line_geom);
