#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/innoz/data/"

psql $db <<EOF
--tables: create relevant tables
--DROP TABLE IF EXISTS bikesharing.stations;
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
--DROP TABLE bikesharing.vehicle_movements;
CREATE TABLE bikesharing.vehicle_movements
  (
    provider text NOT NULL,
    city text NOT NULL,
    key text NOT NULL,
    started_at timestamp without time zone NOT NULL,
    ended_at timestamp without time zone NOT NULL,
    start_station_id integer,
    start_station_name varchar,
    latitude_start numeric,
    longitude_start numeric,
    end_station_id integer,
    end_station_name varchar,
    latitude_end numeric,
    longitude_end numeric,
    stationary boolean NOT NULL DEFAULT false,
    price varchar,
    vehicle_type text NOT NULL DEFAULT 'car'::text,
    from_movements boolean DEFAULT true
  )
;
EOF
