--TODO: integrate tables, merge sightings to movements

-- ---------------------------- --
-- CREATE INDEXES FOR MOVEMENTS --
-- ---------------------------- --
-- index: bikesharing_vehicle_movements_city_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_city_idx;
CREATE INDEX bikesharing_vehicle_movements_city_idx
  ON bikesharing.vehicle_movements
  USING btree
  (city COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_movements_first_seen_at_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_first_seen_at_idx;
CREATE INDEX bikesharing_vehicle_movements_first_seen_at_idx
  ON bikesharing.vehicle_movements
  USING btree
  (first_seen_at);

-- index: bikesharing_vehicle_movements_first_seen_at_idx1
DROP INDEX IF EXISTS bikesharing_vehicle_movements_first_seen_at_idx1;
CREATE INDEX bikesharing_vehicle_movements_first_seen_at_idx1
  ON bikesharing.vehicle_movements
  USING btree
  ((first_seen_at::date));

-- index: bikesharing_vehicle_movements_key_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_key_idx;
CREATE INDEX bikesharing_vehicle_movements_key_idx
  ON bikesharing.vehicle_movements
  USING btree
  (key COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_movements_last_seen_at_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_last_seen_at_idx;
CREATE INDEX bikesharing_vehicle_movements_last_seen_at_idx
  ON bikesharing.vehicle_movements
  USING btree
  (last_seen_at);

-- index: bikesharing_vehicle_movements_last_seen_at_idx1
DROP INDEX IF EXISTS bikesharing_vehicle_movements_last_seen_at_idx1;
CREATE INDEX bikesharing_vehicle_movements_last_seen_at_idx1
  ON bikesharing.vehicle_movements
  USING btree
  ((last_seen_at::date));

-- index: bikesharing_vehicle_movements_latitude_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_latitude_idx;
CREATE INDEX bikesharing_vehicle_movements_latitude_idx
  ON bikesharing.vehicle_movements
  USING btree
  (latitude);

-- index: bikesharing_vehicle_movements_longitude_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_longitude_idx;
CREATE INDEX bikesharing_vehicle_movements_longitude_idx
  ON bikesharing.vehicle_movements
  USING btree
  (longitude);

-- index: bikesharing_vehicle_movements_provider_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_provider_idx;
CREATE INDEX bikesharing_vehicle_movements_provider_idx
  ON bikesharing.vehicle_movements
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_movements_stationary_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_stationary_idx;
CREATE INDEX bikesharing_vehicle_movements_stationary_idx
  ON bikesharing.vehicle_movements
  USING btree
  (stationary);

-- index: bikesharing_vehicle_movements_vehicle_type_idx
DROP INDEX IF EXISTS bikesharing_vehicle_movements_vehicle_type_idx;
CREATE INDEX bikesharing_vehicle_movements_vehicle_type_idx
  ON bikesharing.vehicle_movements
  USING btree
  (vehicle_type COLLATE pg_catalog."default");

  -- ---------------------------- --
  -- CREATE INDEXES FOR SIGHTINGS --
  -- ---------------------------- --
-- index: bikesharing_vehicle_sightings_city_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_city_idx;
CREATE INDEX bikesharing_vehicle_sightings_city_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (city COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_first_seen_at_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_first_seen_at_idx;
CREATE INDEX bikesharing_vehicle_sightings_first_seen_at_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (first_seen_at);

-- index: bikesharing_vehicle_sightings_first_seen_at_idx1
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_first_seen_at_idx1;
CREATE INDEX bikesharing_vehicle_sightings_first_seen_at_idx1
  ON bikesharing.vehicle_sightings
  USING btree
  ((first_seen_at::date));

-- index: bikesharing_vehicle_sightings_key_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_key_idx;
CREATE INDEX bikesharing_vehicle_sightings_key_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (key COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_last_seen_at_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_last_seen_at_idx;
CREATE INDEX bikesharing_vehicle_sightings_last_seen_at_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (last_seen_at);

-- index: bikesharing_vehicle_sightings_last_seen_at_idx1
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_last_seen_at_idx1;
CREATE INDEX bikesharing_vehicle_sightings_last_seen_at_idx1
  ON bikesharing.vehicle_sightings
  USING btree
  ((last_seen_at::date));

-- index: bikesharing_vehicle_sightings_latitude_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_latitude_idx;
CREATE INDEX bikesharing_vehicle_sightings_latitude_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (latitude);

-- index: bikesharing_vehicle_sightings_longitude_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_longitude_idx;
CREATE INDEX bikesharing_vehicle_sightings_longitude_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (longitude);

-- index: bikesharing_vehicle_sightings_provider_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_provider_idx;
CREATE INDEX bikesharing_vehicle_sightings_provider_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_stationary_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_stationary_idx;
CREATE INDEX bikesharing_vehicle_sightings_stationary_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (stationary);

-- index: bikesharing_vehicle_sightings_vehicle_type_idx
DROP INDEX IF EXISTS bikesharing_vehicle_sightings_vehicle_type_idx;
CREATE INDEX bikesharing_vehicle_sightings_vehicle_type_idx
  ON bikesharing.vehicle_sightings
  USING btree
  (vehicle_type COLLATE pg_catalog."default");
