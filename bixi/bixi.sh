#!/bin/bash

db="-p 5432 -d postgres"
folder="/home/bbock/Repositories/Bikesharing/bixi/data/"
url="https://montreal.bixi.com/c/bixi/file_db/data_all.file/"

#create empty table
psql $db <<EOF
DROP TABLE vehicle_movements_bixi;
CREATE TABLE vehicle_movements_bixi
  (
    provider text NOT NULL,
    city text NOT NULL,
    key,
    started_at timestamp without time zone NOT NULL,
    ended_at timestamp without time zone NOT NULL,
    endstation_id integer,
    startstation_id integer,
    fuel_level_start integer,
    fuel_level_end integer,
    stationary boolean NOT NULL DEFAULT false,
    price integer,
    vehicle_type text NOT NULL DEFAULT 'car'::text,
    from_movements boolean DEFAULT true
  )
;
EOF

readarray -t linknames < linknames.csv

for linkname in "${linknames[@]}"
  do
  rm ${folder}${linkname}
  wget ${url}${linkname} -P ${folder}
  unzip ${folder}${linkname} -d ${folder}
  #TODO: mv files from subfolders
  rm ${folder}${linkname}
done

ls -R ${folder} > filenames.csv
readarray -t filenames < filenames.csv

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
  INSERT INTO vehicle_movements_bixi
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      endstation_id,
      startstation_id,
      fuel_level_start,
      fuel_level_end,
      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'bixi' AS provider,
      'montreal' AS city,
      NULL AS key,
      start_date AS started_at,
      end_date AS ended_at,
      start_station_code,
      end_station_code,
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
