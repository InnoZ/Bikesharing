#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/citi_bike/data/"
url="https://s3.amazonaws.com/tripdata/"

# import data for nyc citi bike bikesharing
readarray -t linknames < ${folder}linknames.csv

for linkname in "${linknames[@]}"
  do
  echo "downloading ${folder}${linkname}"
  rm ${folder}${linkname}
  wget ${url}${linkname} -P ${folder}
  unzip ${folder}${linkname} -d ${folder}
  rm ${folder}${linkname}
done

rm filenames.csv
ls -R ${folder} > ${folder}filenames.csv
#TODO: check datestyle for each file
readarray -t filenames < ${folder}filenames.csv

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
  INSERT INTO bikesharing.vehicle_movements
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      start_station_id,
      start_station_name,
      latitude_start,
      longitude_start,
      end_station_id,
      end_station_name,
      latitude_end,
      longitude_end,
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
      start_station_id,
      start_station_name,
      start_station_latitude AS latitude_start,
      start_station_longitude AS longitude_start,
      end_station_id,
      end_station_name,
      end_station_latitude AS latitude_end,
      end_station_longitude AS longitude_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
done

readarray -t filenames_MDY < ${folder}filenames_MDY.csv
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
  INSERT INTO bikesharing.vehicle_movements
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      start_station_id,
      start_station_name,
      latitude_start,
      longitude_start,
      end_station_id,
      end_station_name,
      latitude_end,
      longitude_end,
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
      start_station_id,
      start_station_name,
      start_station_latitude AS latitude_start,
      start_station_longitude AS longitude_start,
      end_station_id,
      end_station_name,
      end_station_latitude AS latitude_end,
      end_station_longitude AS longitude_end,
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
