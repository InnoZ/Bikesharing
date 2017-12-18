-- select extract and calculate additional attributes
-- DROP TABLE IF EXISTS bikesharing.vehicle_sightings_year;
CREATE TABLE bikesharing.vehicle_sightings_year AS
SELECT *
FROM bikesharing.vehicle_sightings_clean
WHERE first_seen_at BETWEEN '2016-12-01 00:00' AND '2017-12-01 00:00';

-- index: bikesharing_vehicle_sightings_year_city_idx

DROP INDEX bikesharing_vehicle_sightings_year_city_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_city_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (city COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_year_first_seen_at_idx

DROP INDEX bikesharing_vehicle_sightings_year_first_seen_at_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_first_seen_at_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (first_seen_at);

-- index: bikesharing_vehicle_sightings_year_first_seen_at_idx1

DROP INDEX bikesharing_vehicle_sightings_year_first_seen_at_idx1;

CREATE INDEX bikesharing_vehicle_sightings_year_first_seen_at_idx1
  ON bikesharing.vehicle_sightings_year
  USING btree
  ((first_seen_at::date));

-- index: bikesharing_vehicle_sightings_year_key_idx

DROP INDEX bikesharing_vehicle_sightings_year_key_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_key_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (key COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_year_last_seen_at_idx

DROP INDEX bikesharing_vehicle_sightings_year_last_seen_at_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_last_seen_at_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (last_seen_at);

-- index: bikesharing_vehicle_sightings_year_last_seen_at_idx1

DROP INDEX bikesharing_vehicle_sightings_year_last_seen_at_idx1;

CREATE INDEX bikesharing_vehicle_sightings_year_last_seen_at_idx1
  ON bikesharing.vehicle_sightings_year
  USING btree
  ((last_seen_at::date));

-- index: bikesharing_vehicle_sightings_year_latitude_idx

DROP INDEX bikesharing_vehicle_sightings_year_latitude_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_latitude_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (latitude);

-- index: bikesharing_vehicle_sightings_year_longitude_idx

DROP INDEX bikesharing_vehicle_sightings_year_longitude_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_longitude_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (longitude);

-- index: bikesharing_vehicle_sightings_year_provider_idx

DROP INDEX bikesharing_vehicle_sightings_year_provider_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_provider_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (provider COLLATE pg_catalog."default");

-- index: bikesharing_vehicle_sightings_year_stationary_idx

DROP INDEX bikesharing_vehicle_sightings_year_stationary_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_stationary_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (stationary);

-- index: bikesharing_vehicle_sightings_year_vehicle_type_idx

DROP INDEX bikesharing_vehicle_sightings_year_vehicle_type_idx;

CREATE INDEX bikesharing_vehicle_sightings_year_vehicle_type_idx
  ON bikesharing.vehicle_sightings_year
  USING btree
  (vehicle_type COLLATE pg_catalog."default");
