#!/bin/bash

db="-p 5432 -d postgres"
user="bbock"

#create empty table
psql $db <<EOF
DROP TABLE vehicle_movements_santander_cycle;
CREATE TABLE vehicle_movements_santander_cycle
  (
    provider text NOT NULL,
    city text NOT NULL,
    key text NOT NULL,
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

# import data for london santander bikesharing
# 'http://cycling.data.tfl.gov.uk/usage-stats/'
readarray -t filenames < filenames.csv

for filename in "${filenames[@]}"
  do
  filename_offline=$(echo "$filename"|awk '{gsub(/%20/," ")}1')
  rm /tmp/${filename_offline}
  wget http://cycling.data.tfl.gov.uk/usage-stats/${filename} -P /tmp/
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
    FROM '/tmp/${filename_offline}'
    WITH DELIMITER AS E',' NULL AS '' csv HEADER
  ;
  INSERT INTO vehicle_movements_santander_cycle
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
      'santander_cycle' AS provider,
      'london' AS city,
      bike_id AS key,
      start_date AS started_at,
      end_date AS ended_at,
      endstation_id,
      endstation_name,
      startstation_id,
      startstation_name,
      NULL AS fuel_level_start,
      NULL AS fuel_level_end,
      TRUE AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
  rm /tmp/${filename_offline}
done
