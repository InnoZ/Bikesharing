#!/bin/bash

db="-p 5432 -d postgres"
folder="/home/bbock/Repositories/Bikesharing/capital_bikeshare/data/"
url="https://s3.amazonaws.com/capitalbikeshare-data/"

#create empty table
psql $db <<EOF
DROP TABLE vehicle_movements_capital_bikeshare;
CREATE TABLE vehicle_movements_capital_bikeshare
  (
    provider text NOT NULL,
    city text NOT NULL,
    key text,
    started_at timestamp without time zone NOT NULL,
    ended_at timestamp without time zone NOT NULL,
    start_station_id integer,
    start_station_name varchar,
    end_station_id integer,
    end_station_name varchar,
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

#for linkname in "${linknames[@]}"
#  do
#  echo "downloading ${folder}${linkname}"
#  rm ${folder}${linkname}
#  wget ${url}${linkname} -P ${folder}
#  unzip ${folder}${linkname} -d ${folder}
#  rm ${folder}${linkname}
#done

#rm filenames.csv
#ls -R ${folder} > filenames.csv
#TODO: check datestyle for each file
readarray -t filenames < filenames.csv

for filename in "${filenames[@]}"
  do
  echo "### copying ${filename}"
  psql $db <<EOF
  SET datestyle = 'ISO, MDY';
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      Duration varchar,
      Start_date timestamp,
      End_date timestamp,
      Start_station varchar,
      End_station varchar,
      Bike varchar,
      Member_Type varchar
    )
  ;
  COPY temp1
    FROM '${folder}${filename}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  INSERT INTO vehicle_movements_capital_bikeshare
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      start_station_id,
      start_station_name,
      end_station_id,
      end_station_name,
      fuel_level_start,
      fuel_level_end,
      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'capital_bikeshare' AS provider,
      'washingtondc' AS city,
      Bike AS key,
      Start_date AS started_at,
      End_date AS ended_at,
      NULL AS start_station_id,
      Start_station AS start_station_name,
      NULL AS end_station_id,
      End_station AS end_station_name,
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

readarray -t filenames_2 < filenames_2.csv
for filename_2 in "${filenames_2[@]}"
  do
  echo "### copying ${filename_2}"
  psql $db <<EOF
  SET datestyle = 'ISO, MDY';
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      Duration varchar,
      Start_date timestamp,
      Start_station varchar,
      End_date timestamp,
      End_station varchar,
      Bike varchar,
      Subscription_Type varchar
    )
  ;
  COPY temp1
    FROM '${folder}${filename_2}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  INSERT INTO vehicle_movements_capital_bikeshare
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      start_station_id,
      start_station_name,
      end_station_id,
      end_station_name,
      fuel_level_start,
      fuel_level_end,
      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'capital_bikeshare' AS provider,
      'washingtondc' AS city,
      Bike AS key,
      Start_date AS started_at,
      End_date AS ended_at,
      NULL AS start_station_id,
      Start_station AS start_station_name,
      NULL AS end_station_id,
      End_station AS end_station_name,
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

readarray -t filenames_3 < filenames_3.csv
for filename_2 in "${filenames_3[@]}"
  do
  echo "### copying ${filename_2}"
  psql $db <<EOF
  SET datestyle = 'ISO, MDY';
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      Duration varchar,
      Start_date timestamp,
      End_date timestamp,
      Start_station_id integer,
      Start_station varchar,
      End_station_id integer,
      End_station varchar,
      Bike varchar,
      Member_Type varchar
    )
  ;
  COPY temp1
    FROM '${folder}${filename_2}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  INSERT INTO vehicle_movements_capital_bikeshare
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      start_station_id,
      start_station_name,
      end_station_id,
      end_station_name,
      fuel_level_start,
      fuel_level_end,
      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'capital_bikeshare' AS provider,
      'washingtondc' AS city,
      Bike AS key,
      Start_date AS started_at,
      End_date AS ended_at,
      Start_station_id AS start_station_id,
      Start_station AS start_station_name,
      End_station_id AS end_station_id,
      End_station AS end_station_name,
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
#rm ${folder}*.csv
