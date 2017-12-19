#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/bixi/data/"
url="https://montreal.bixi.com/c/bixi/file_db/data_all.file/"

# # get trip data from open data protal
# readarray -t linknames < ${folder}linknames.csv
#
# for linkname in "${linknames[@]}"
#   do
#   echo "downloading ${folder}${linkname}"
#   rm ${folder}${linkname}
#   wget ${url}${linkname} -P ${folder}
#   unzip ${folder}${linkname} -d ${folder}
#   #TODO: mv files from subfolders
#   rm ${folder}${linkname}
# done
# # flatten files in folders
# find ${folder} -mindepth 2 -type f -exec mv -i '{}' ${folder} ';'
# # create list of files
# ls -R ${folder} > ${folder}filenames.csv
readarray -t filenames_stations < ${folder}filenames_stations.csv
readarray -t filenames < ${folder}filenames.csv

# import station details
for filename_stations in "${filenames_stations[@]}"
  do
  psql $db <<EOF
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE temp1
    (
      code integer,
      name varchar,
      latitude numeric,
      longitude numeric
    )
  ;
  COPY temp1
    FROM '${folder}${filename_stations}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  DELETE FROM temp1
  WHERE code IN
    (
      SELECT station_id
      FROM bikesharing.stations
      WHERE provider='bixi'
    )
  ;
  INSERT INTO bikesharing.stations
    (
      provider,
      city,
      station_id,
      station_name,
      latitude,
      longitude,
      vehicle_type,
      from_movements
    )
    SELECT
         'bixi' AS provider,
         'montreal' AS city,
         code AS station_id,
         name AS station_name,
         latitude AS latitude,
         longitude AS longitude,
         'bike' AS vehicle_type,
         TRUE AS from_movements
    FROM temp1
  ;
EOF
done

# import trips
for filename in "${filenames[@]}"
  do
  psql $db <<EOF
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      start_date timestamp,
      start_station_code integer,
      end_date  timestamp,
      end_station_code integer,
      duration_sec integer,
      is_member boolean
    )
  ;
  COPY temp1
    FROM '${folder}${filename}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  DROP TABLE IF EXISTS temp2;
  CREATE TABLE temp2 AS
  SELECT
    trips.start_date AS started_at,
    trips.end_date AS ended_at,
    trips.start_station_code AS start_station_id,
    stations.latitude AS latitude_start,
    stations.longitude AS longitude_start,
    trips.end_station_code AS end_station_id
  FROM temp1 trips
  LEFT OUTER JOIN
    (
      SELECT * FROM bikesharing.stations WHERE provider='bixi'
    ) stations
    ON
    (
      trips.start_station_code=stations.station_id
    )
  ;
  DROP TABLE IF EXISTS temp3;
  CREATE TABLE temp3 AS
  SELECT
    trips.started_at,
    trips.ended_at,
    trips.start_station_id,
    trips.latitude_start,
    trips.longitude_start,
    trips.end_station_id,
    stations.latitude AS latitude_end,
    stations.longitude AS longitude_end
  FROM temp2 trips
  LEFT OUTER JOIN
    (
      SELECT * FROM bikesharing.stations WHERE provider='bixi'
    ) stations
    ON
    (
      trips.end_station_id=stations.station_id
    )
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
      'bixi' AS provider,
      'montreal' AS city,
      NULL AS key,
      started_at,
      ended_at,
      start_station_id,
      NULL AS start_station_name,
      NULL AS latitude_start,
      NULL AS longitude_start,
      end_station_id,
      NULL AS end_station_name,
      NULL AS latitude_end,
      NULL AS longitude_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp3
  ;
EOF
done
