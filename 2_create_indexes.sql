--TODO: integrate tables, merge sightings to movements

-- select extract and calculate additional attributes
-- DROP TABLE IF EXISTS bikesharing.vehicle_sightings_clean;
CREATE TABLE bikesharing.vehicle_sightings_clean AS
SELECT *
FROM bikesharing.vehicle_sightings;

-- index: bikesharing_vehicle_sightings_clean_city_idx

DROP INDEX bikesharing_vehicle_sightings_clean_city_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_city_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (city COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_clean_first_seen_at_idx

DROP INDEX bikesharing_vehicle_sightings_clean_first_seen_at_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_first_seen_at_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (first_seen_at);

-- index: bikesharing_vehicle_sightings_clean_first_seen_at_idx1

DROP INDEX bikesharing_vehicle_sightings_clean_first_seen_at_idx1;

CREATE INDEX bikesharing_vehicle_sightings_clean_first_seen_at_idx1
  ON bikesharing.vehicle_sightings_clean
  USING btree
  ((first_seen_at::date));

-- index: bikesharing_vehicle_sightings_clean_key_idx

DROP INDEX bikesharing_vehicle_sightings_clean_key_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_key_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (key COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_clean_last_seen_at_idx

DROP INDEX bikesharing_vehicle_sightings_clean_last_seen_at_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_last_seen_at_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (last_seen_at);

-- index: bikesharing_vehicle_sightings_clean_last_seen_at_idx1

DROP INDEX bikesharing_vehicle_sightings_clean_last_seen_at_idx1;

CREATE INDEX bikesharing_vehicle_sightings_clean_last_seen_at_idx1
  ON bikesharing.vehicle_sightings_clean
  USING btree
  ((last_seen_at::date));

-- index: bikesharing_vehicle_sightings_clean_latitude_idx

DROP INDEX bikesharing_vehicle_sightings_clean_latitude_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_latitude_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (latitude);

-- index: bikesharing_vehicle_sightings_clean_longitude_idx

DROP INDEX bikesharing_vehicle_sightings_clean_longitude_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_longitude_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (longitude);

-- index: bikesharing_vehicle_sightings_clean_provider_idx

DROP INDEX bikesharing_vehicle_sightings_clean_provider_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_provider_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_clean_stationary_idx

DROP INDEX bikesharing_vehicle_sightings_clean_stationary_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_stationary_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (stationary);

-- index: bikesharing_vehicle_sightings_clean_vehicle_type_idx

DROP INDEX bikesharing_vehicle_sightings_clean_vehicle_type_idx;

CREATE INDEX bikesharing_vehicle_sightings_clean_vehicle_type_idx
  ON bikesharing.vehicle_sightings_clean
  USING btree
  (vehicle_type COLLATE pg_catalog."default");
