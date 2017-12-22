#!/bin/bash

user="user"
db="-p 5432 -d shared_mobility"
folder="/home/${user}Bikesharing/data_import/santander_cycle/data/"
url="http://cycling.data.tfl.gov.uk/usage-stats/"

# import data for london santander bikesharing
# 'http://cycling.data.tfl.gov.uk/usage-stats/'
readarray -t filenames < ${folder}filenames.csv

for filename in "${filenames[@]}"
  do
  filename_offline=$(echo "$filename"|awk '{gsub(/%20/," ")}1')
  rm ${folder}${filename_offline}
  wget ${url}${filename} -P ${folder}
  #create empty table
  #data format:
  #Rental Id,Duration,Bike Id,End Date,EndStation Id,EndStation Name,Start Date,StartStation Id,StartStation Name
  #50754225,240,11834,10/01/2016 00:04,383,"Frith Street, Soho",10/01/2016 00:00,18,"Drury Lane, Covent Garden"
  psql $db <<EOF
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      rental_id integer,
      duration integer,
      bike_id integer,
      end_date timestamp,
      endstation_id integer,
      endstation_name varchar,
      start_date timestamp,
      startstation_id integer,
      startstation_name varchar
    )
  ;
  COPY temp1
    FROM '${folder}${filename_offline}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
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
      'santander_cycle' AS provider,
      'london' AS city,
      bike_id AS key,
      start_date AS started_at,
      end_date AS ended_at,
      startstation_id,
      startstation_name,
      NULL AS latitude_start,
      NULL AS longitude_start,
      endstation_id,
      endstation_name,
      NULL AS latitude_end,
      NULL AS longitude_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
  #rm ${folder}${filename_offline}
done
