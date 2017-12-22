#!/bin/bash

user="user"
db="-p 5432 -d shared_mobility"
folder="/home/${user}Bikesharing/data_import/divvy_bike/data/"
url="https://s3.amazonaws.com/divvy-data/tripdata/"


readarray -t filenames < ${folder}filenames.csv

for filename in "${filenames[@]}"
do
  # get data for Chicago bikesharing
  rm ${folder}${filename}.zip
  wget ${url}${filename}.zip -P ${folder}
  unzip ${folder}${filename}.zip -d ${folder}
  rm ${folder}${filename}*.zip
  rm ${folder}README*.txt

#remove quotes from csv
#sed -i 's/"//g' ${folder}OPENDATA_BOOKING_CALL_A_BIKE.csv

trip_id	starttime	stoptime	bikeid	tripduration	from_station_id	from_station_name	to_station_id	to_station_name	usertype	gender
4118	2013-06-27 12:11	2013-06-27 12:16	480	316	85	Michigan Ave & Oak St	28	Larrabee St & Menomonee St	Customer

#create empty table
psql $db <<EOF

  DROP TABLE vehicle_movements_divvy;
  CREATE TABLE vehicle_movements_divvy
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
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
      trip_id	integer,
      starttime timestamp,
      stoptime timestamp,
      bikeid	tripduration	from_station_id	from_station_name	to_station_id	to_station_name	usertype	gender
      BOOKING_HAL_ID integer,
      CATEGORY_HAL_ID integer,
      VEHICLE_HAL_ID integer,
      CUSTOMER_HAL_ID varchar,
      DATE_BOOKING timestamp,
      DATE_FROM timestamp,
      DATE_UNTIL timestamp,
      COMPUTE_EXTRA_BOOKING_FEE varchar,
      TRAVERSE_USE varchar,
      DISTANCE numeric,
      START_RENTAL_ZONE varchar,
      START_RENTAL_ZONE_HAL_ID varchar,
      END_RENTAL_ZONE varchar,
      END_RENTAL_ZONE_HAL_ID varchar,
      RENTAL_ZONE_HAL_SRC varchar,
      CITY_RENTAL_ZONE varchar,
      TECHNICAL_INCOME_CHANNEL varchar
    )
  ;
  COPY temp1
    FROM '${folder}OPENDATA_BOOKING_CALL_A_BIKE.csv'
    WITH DELIMITER AS E';' NULL AS '' csv HEADER
  ;
  INSERT INTO vehicle_movements_divvy
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


      stationary,
      price,
      vehicle_type,
      from_movements
    )
    SELECT
      'divvy' AS provider,
      'chicago' AS city,
      VEHICLE_HAL_ID,
      DATE_FROM,
      DATE_UNTIL,
      END_RENTAL_ZONE_HAL_ID::integer,
      END_RENTAL_ZONE,
      START_RENTAL_ZONE_HAL_ID::integer,
      START_RENTAL_ZONE,

      
      CASE WHEN RENTAL_ZONE_HAL_SRC='standort' THEN TRUE ELSE FALSE END AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
done
