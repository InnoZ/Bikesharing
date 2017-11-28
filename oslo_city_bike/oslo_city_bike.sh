#!/bin/bash

db="-p 5432 -d postgres"
path="/home/bbock/Repositories/Bikesharing/oslo_city_bike/data/"
url="https://developer.oslobysykkel.no/data/"

#create empty table
psql $db <<EOF
DROP TABLE vehicle_movements_oslo_city_bike;
CREATE TABLE vehicle_movements_oslo_city_bike
  (
    provider text NOT NULL,
    city text NOT NULL,
    key text,
    started_at timestamp without time zone NOT NULL,
    ended_at timestamp without time zone NOT NULL,
    endstation_id integer,
    endstation_name varchar,
    startstation_id integer,
    startstation_name varchar,
    fuel_level_start integer,
    fuel_level_end integer,
    stationary boolean NOT NULL DEFAULT false,
    price integer,
    vehicle_type text NOT NULL DEFAULT 'car'::text,
    from_movements boolean DEFAULT true
  )
;
EOF

# import data for oslo
readarray -t linknames < linknames.csv

for linkname in "${linknames[@]}"
  do
  rm "${path}""${linkname}".csv
  wget "${url}""${linkname}".csv -P "${path}"
  unzip "${path}""${linkname}".csv -d "${path}"
  rm "${path}""${linkname}".csv
done

ls -R "${path}" > filenames.csv
readarray -t filenames < filenames.csv

for filename in "${filenames[@]}"
  do
  chmod a+rwx "${path}""${filename}"
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
    FROM '${path}${filename}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  INSERT INTO vehicle_movements_oslo_city_bike
    (
      provider,
      city,
      key,
      started_at,
      ended_at,
      endstation_id,
      endstation_name,
      startstation_id,
      startstation_name,
      fuel_level_start,
      fuel_level_end,
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
      end_station,
      NULL AS endstation_name,
      start_station,
      NULL AS startstation_name,
      NULL AS fuel_level_start,
      NULL AS fuel_level_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
  #rm "${path}""${filename}"
done
