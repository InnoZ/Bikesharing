--tables: create relevant tables
DROP TABLE IF EXISTS bikesharing.stations;
CREATE TABLE bikesharing.stations
  (
    provider varchar,
    city varchar,
    station_id integer,
    station_name varchar,
    latitude numeric,
    longitude numeric,
    vehicle_type  varchar,
    from_movements boolean
  )
;
DROP TABLE bikesharing.vehicle_movements;
CREATE TABLE bikesharing.vehicle_movements
  (
    provider text,
    city text,
    key text,
    started_at timestamp without time zone,
    ended_at timestamp without time zone,
    start_station_id integer,
    start_station_name varchar,
    latitude_start numeric,
    longitude_start numeric,
    end_station_id integer,
    end_station_name varchar,
    latitude_end numeric,
    longitude_end numeric,
    stationary boolean  DEFAULT false,
    price varchar,
    vehicle_type text  DEFAULT 'car'::text,
    from_movements boolean DEFAULT true
  )
;
