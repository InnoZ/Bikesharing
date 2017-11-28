#!/bin/bash

db="-p 5432 -d postgres"
folder="/home/bbock/Repositories/Bikesharing/citi_bike/data/"
url="https://s3.amazonaws.com/tripdata/"

#create empty table
psql $db <<EOF
DROP TABLE vehicle_movements_citi_bike;
CREATE TABLE vehicle_movements_citi_bike
  (
    provider text NOT NULL,
    city text NOT NULL,
    key text NOT NULL,
    started_at timestamp without time zone NOT NULL,
    ended_at timestamp without time zone NOT NULL,
    latitude_start numeric(11,8) NOT NULL,
    longitude_start numeric(11,8) NOT NULL,
    latitude_end numeric(11,8) NOT NULL,
    longitude_end numeric(11,8) NOT NULL,
    fuel_level_start integer,
    fuel_level_end integer,
    stationary boolean NOT NULL DEFAULT false,
    price integer,
    vehicle_type text NOT NULL DEFAULT 'car'::text,
    from_movements boolean DEFAULT true
  )
;
EOF

# import data for nyc citi bike bikesharing
readarray -t linknames < linknames.csv

for linkname in "${linknames[@]}"
  do
  echo "downloading ${folder}${linkname}"
  rm ${folder}${linkname}
  wget ${url}${linkname} -P ${folder}
  unzip ${folder}${linkname} -d ${folder}
  rm ${folder}${linkname}
done

#rm filenames.csv
#ls -R ${folder} > filenames.csv
#TODO: check datestyle for each file
readarray -t filenames < filenames.csv

for filename in "${filenames[@]}"
  do
  echo "### copying ${filename}"
  psql $db <<EOF
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      tripduration integer,
      starttime timestamp,
      stoptime timestamp,
      start_station_id integer,
      start_station_name varchar,
      start_station_latitude numeric,
      start_station_longitude numeric,
      end_station_id integer,
      end_station_name varchar,
      end_station_latitude numeric,
      end_station_longitude numeric,
      bikeid integer,
      usertype varchar,
      birth_year varchar,
      gender varchar
    )
  ;
  COPY temp1
    FROM '${folder}${filename}'
    WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER
  ;
  INSERT INTO vehicle_movements_citi_bike
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      latitude_start,
      longitude_start,
      latitude_end,
      longitude_end,
      fuel_level_start,
      fuel_level_end,
      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'citi_bike' AS provider,
      'newyorkcity' AS city,
      bikeid AS key,
      starttime AS started_at,
      stoptime AS ended_at,
      start_station_latitude AS latitude_start,
      start_station_longitude AS longitude_start,
      end_station_latitude AS latitude_end,
      end_station_longitude AS longitude_end,
      NULL AS fuel_level_start,
      NULL AS fuel_level_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
done

readarray -t filenames_MDY < filenames_MDY.csv
for filename_MDY in "${filenames_MDY[@]}"
  do
  echo "### copying ${filename_MDY}"
  psql $db <<EOF
  SET datestyle = 'ISO, MDY';
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      tripduration integer,
      starttime timestamp,
      stoptime timestamp,
      start_station_id integer,
      start_station_name varchar,
      start_station_latitude numeric,
      start_station_longitude numeric,
      end_station_id integer,
      end_station_name varchar,
      end_station_latitude numeric,
      end_station_longitude numeric,
      bikeid integer,
      usertype varchar,
      birth_year varchar,
      gender varchar
    )
  ;
  COPY temp1
    FROM '${folder}${filename_MDY}'
    WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER
  ;
  INSERT INTO vehicle_movements_citi_bike
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      latitude_start,
      longitude_start,
      latitude_end,
      longitude_end,
      fuel_level_start,
      fuel_level_end,
      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'citi_bike' AS provider,
      'newyorkcity' AS city,
      bikeid AS key,
      starttime AS started_at,
      stoptime AS ended_at,
      start_station_latitude AS latitude_start,
      start_station_longitude AS longitude_start,
      end_station_latitude AS latitude_end,
      end_station_longitude AS longitude_end,
      NULL AS fuel_level_start,
      NULL AS fuel_level_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
  SET datestyle = 'ISO, DMY';
EOF
done
#rm ${folder}*.csv
