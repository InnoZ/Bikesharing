#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/oslo_city_bike/data/"
url="https://developer.oslobysykkel.no/data/"

# #get trip data from open data portal
# readarray -t linknames < ${folder}linknames.csv
# for linkname in ${linknames[@]}
#   do
#   rm ${folder}${linkname}.csv
#   wget ${url}${linkname}.csv -P ${folder}
#   unzip ${folder}${linkname}.csv -d ${folder}
#   rm ${folder}${linkname}.csv
# done
#
# ls -R ${folder} > ${folder}filenames.csv
readarray -t filenames < ${folder}filenames.csv

for filename in ${filenames[@]}
  do
  # change reading rights to files
  chmod a+rwx ${folder}${filename}
  #create empty table
  #data format:
  #Rental Id,Duration,Bike Id,End Date,EndStation Id,EndStation Name,Start Date,StartStation Id,StartStation Name
  #50754225,240,11834,10/01/2016 00:04,383,"Frith Street, Soho",10/01/2016 00:00,18,"Drury Lane, Covent Garden"
  psql $db <<EOF
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      start_station integer,
      start_time timestamp,
      end_station integer,
      end_time timestamp
    )
  ;
  COPY temp1
    FROM '${folder}${filename}'
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
      'oslo_city_bike' AS provider,
      'oslo' AS city,
      NULL AS key,
      start_time AS started_at,
      end_time AS ended_at,
      start_station AS start_station_id,
      NULL AS start_station_name,
      NULL AS latitude_start,
      NULL AS longitude_start,
      end_station AS end_station_id,
      NULL AS end_station_name,
      NULL AS latitude_end,
      NULL AS longitude_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
  #rm "${folder}""${filename}"
done
