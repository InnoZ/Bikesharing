#!/bin/bash

#!/bin/bash

db="-p 5432 -d postgres"
folder="/home/bbock/Repositories/Bikesharing/call_a_bike/data/"
url="http://download-data.deutschebahn.com/static/datasets/callabike/"

#import stations
rm ${folder}OPENDATA_BOOKING_CALL_A_BIKE.zip
wget "${url}"20170516/OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip -P $folder
rm ${folder}OPENDATA_BOOKING_CALL_A_BIKE.csv
unzip ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip -d $folder
#remove quotes from csv TODO: is there a more elegant way?
sed -i 's/"//g' ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv
sed -i 's/,/./g' ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv
#create empty table
psql $db <<EOF
    -- import station data for callabike bikesharing
    -- 'http://download-data.deutschebahn.com'
    DROP TABLE IF EXISTS temp2;
    CREATE TABLE temp2
      (
        RENTAL_ZONE_HAL_ID integer,
        RENTAL_ZONE_HAL_SRC varchar,
        NAME varchar,
        CODE bigint,
        TYPE varchar,
        CITY varchar,
        COUNTRY varchar,
        LATITUDE numeric,
        LONGITUDE numeric,
        POI_AIRPORT_X varchar,
        POI_LONG_DISTANCE_TRAINS_X varchar,
        POI_SUBURBAN_TRAINS_X varchar,
        POI_UNDERGROUND_X varchar,
        ACTIVE_X varchar,
        COMPANY  varchar,
        COMPANY_GROUP varchar
        )
      ;
    COPY temp2
      FROM '${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv'
      WITH DELIMITER AS E';' NULL AS '' csv HEADER
    ;
EOF

#get file from open data protal
rm ${folder}OPENDATA_BOOKING_CALL_A_BIKE.zip
wget ${url}20170516/OPENDATA_BOOKING_CALL_A_BIKE.zip -P $folder
unzip ${folder}OPENDATA_BOOKING_CALL_A_BIKE.zip -d $folder
#remove quotes from csv TODO: is there a more elegant way?
sed -i 's/"//g' ${folder}OPENDATA_BOOKING_CALL_A_BIKE.csv

#create empty table
psql $db <<EOF
  DROP TABLE vehicle_movements_callabike;
  CREATE TABLE vehicle_movements_callabike
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
  -- import data for callabike bikesharing
  -- 'http://download-data.deutschebahn.com'
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE IF NOT EXISTS temp1
    (
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
  INSERT INTO vehicle_movements_callabike
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
      'call_a_bike' AS provider,
      lower(city_rental_zone) AS city,
      VEHICLE_HAL_ID,
      DATE_FROM,
      DATE_UNTIL,
      END_RENTAL_ZONE_HAL_ID::integer,
      END_RENTAL_ZONE,
      START_RENTAL_ZONE_HAL_ID::integer,
      START_RENTAL_ZONE,
      NULL AS fuel_level_start,
      NULL AS fuel_level_end,
      CASE WHEN RENTAL_ZONE_HAL_SRC='standort' THEN TRUE ELSE FALSE END AS stationary,
      NULL AS price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF
