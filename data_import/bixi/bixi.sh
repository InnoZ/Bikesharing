#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/bixi/data/"
url="https://montreal.bixi.com/c/bixi/file_db/data_all.file/"
url_stations="https://secure.bixi.com/data/"

# # datasource: https://montreal.bixi.com/en/open-data
# # import stations
# rm ${folder}stations.json
# wget ${url_stations}stations.json -P ${folder}
# # TODO: ad capacity to station table
# psql $db <<EOF
# DROP TABLE IF EXISTS temp1;
# CREATE TABLE temp1
#   (
#     id integer,
#     s varchar,
#     n integer,
#     st integer,
#     b boolean,
#     su boolean,
#     m boolean,
#     lu bigint,
#     lc bigint,
#     bk boolean,
#     bl boolean,
#     la numeric,
#     lo numeric,
#     da integer,
#     dx integer,
#     ba integer,
#     bx integer
#   )
# ;
# COPY temp1
#   FROM '${folder}stations.json'
#   DELIMITER AS E','
# ;
# DELETE FROM temp1
# WHERE n IN
#   (
#     SELECT station_id
#     FROM bikesharing.stations
#     WHERE provider='bixi'
#   )
# ;
# INSERT INTO bikesharing.stations
#   (
#     provider,
#     city,
#     station_id,
#     station_name,
#     latitude,
#     longitude,
#     vehicle_type,
#     from_movements
#   )
#   SELECT
#     'bixi' AS provider,
#     'montreal' AS city,
#     s AS station_id,
#     n AS station_name,
#     la AS latitude,
#     lo AS longitude,
#     'bike' AS vehicle_type,
#     TRUE AS from_movements
#   FROM temp1
# ;
# EOF

#get trip data from open data protal
readarray -t linknames < ${folder}linknames.csv
for linkname in "${linknames[@]}"
  do
  rm ${folder}${linkname}
  wget ${url}${linkname} -P ${folder}
  unzip ${folder}${linkname} -d ${folder}
  #TODO: mv files from subfolders
  rm ${folder}${linkname}
done

ls -R ${folder} > ${folder}filenames.csv
readarray -t filenames < ${folder}filenames.csv

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
    --stations.la AS latitude_start,
    --stations.lo AS longitude_start,
    trips.end_station_code AS end_station_id
  FROM temp1 trips
  --LEFT OUTER JOIN
  --  (
  --    SELECT * FROM bikesharing.stations WHERE provider='bixi'
  --  ) stations
  --  ON
  --  (
  --    trips.start_station_code=stations.station_id
  --  )
  ;
  DROP TABLE IF EXISTS temp3;
  CREATE TABLE temp3 AS
  SELECT
    trips.started_at,
    trips.ended_at,
    trips.start_station_id,
    --trips.latitude_start,
    --trips.longitude_start,
    trips.end_station_id,
    --stations.la latitude_end,
    --stations.lo AS longitude_end
  FROM temp2 trips
  --LEFT OUTER JOIN
  --  (
  --    SELECT * FROM bikesharing.stations WHERE provider='bixi'
  --  ) stations
  --  ON
  --  (
  --    trips.end_station_id=stations.station_id
  --  )
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
