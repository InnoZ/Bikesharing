#!/bin/bash

db="-p 5432 -d shared_mobility"
folder="/home/bbock/Repositories/Bikesharing/data_import/call_a_bike/data/"
url="http://download-data.deutschebahn.com/static/datasets/callabike/"

# datasource: http://data.deutschebahn.com/dataset/data-call-a-bike
#import stations
rm ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip
wget "${url}"20170516/OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip -P $folder
rm ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv
unzip ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip -d $folder
rm ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.zip
#remove quotes from csv TODO: is there a more elegant way?
sed -i 's/"//g' ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv
sed -i 's/,/./g' ${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv
psql $db <<EOF
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE temp1
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
  COPY temp1
    FROM '${folder}OPENDATA_RENTAL_ZONE_CALL_A_BIKE.csv'
    WITH DELIMITER AS E';' NULL AS '' csv HEADER
  ;
  DELETE FROM temp1
  WHERE RENTAL_ZONE_HAL_ID IN
    (
      SELECT station_id
      FROM bikesharing.stations
      WHERE provider='call_a_bike'
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
      'call_a_bike' AS provider,
      lower(CITY) AS city,
      RENTAL_ZONE_HAL_ID AS station_id,
      NAME AS station_name,
      LATITUDE AS latitude,
      LONGITUDE AS longitude,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF

#import more stations
rm ${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.zip
wget "${url}"20160607/HACKATHON_RENTAL_ZONE_CALL_A_BIKE.zip -P $folder
rm ${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.csv
unzip ${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.zip -d $folder
rm ${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.zip
#remove quotes from csv TODO: is there a more elegant way?
sed -i 's/"//g' ${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.csv
sed -i 's/,/./g' ${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.csv
#create empty table
psql $db <<EOF
  -- import station data for callabike bikesharing
  -- 'http://download-data.deutschebahn.com'
  DROP TABLE IF EXISTS temp1;
  CREATE TABLE temp1
    (
      RENTAL_ZONE_HAL_ID integer,
      RENTAL_ZONE_HAL_SRC varchar,
      NAME varchar,
      TYPE varchar,
      CITY varchar,
      COUNTRY varchar,
      POI_AIRPORT_X varchar,
      POI_LONG_DISTANCE_TRAINS_X varchar,
      POI_SUBURBAN_TRAINS_X varchar,
      POI_UNDERGROUND_X varchar,
      CLASSIFICATION  varchar,
      RENTAL_ZONE_GROUP  varchar,
      CODE bigint,
      COMPANY  varchar,
      COMPANY_GROUP varchar,
      FRANCHISE varchar,
      ACTIVE_X varchar,
      RENTAL_ZONE_GROUP_X varchar,
      RENTAL_ZONE_X_COORDINATE numeric,
      RENTAL_ZONE_Y_COORDINATE numeric
      )
    ;
  COPY temp1
    FROM '${folder}HACKATHON_RENTAL_ZONE_CALL_A_BIKE.csv'
    WITH DELIMITER AS E';' NULL AS '' csv HEADER
  ;
  DELETE FROM temp1
  WHERE RENTAL_ZONE_HAL_ID IN
    (
      SELECT station_id
      FROM bikesharing.stations
      WHERE provider='call_a_bike'
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
      'call_a_bike' AS provider,
      lower(CITY) AS city,
      RENTAL_ZONE_HAL_ID AS station_id,
      NAME AS station_name,
      RENTAL_ZONE_Y_COORDINATE AS latitude,
      RENTAL_ZONE_X_COORDINATE AS longitude,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp1
  ;
EOF

#get trip data from open data portal
rm ${folder}OPENDATA_BOOKING_CALL_A_BIKE.zip
wget ${url}20170516/OPENDATA_BOOKING_CALL_A_BIKE.zip -P $folder
unzip ${folder}OPENDATA_BOOKING_CALL_A_BIKE.zip -d $folder
#remove quotes from csv TODO: is there a more elegant way?
sed -i 's/"//g' ${folder}OPENDATA_BOOKING_CALL_A_BIKE.csv
psql $db <<EOF
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
      START_RENTAL_ZONE_HAL_ID integer,
      END_RENTAL_ZONE varchar,
      END_RENTAL_ZONE_HAL_ID integer,
      RENTAL_ZONE_HAL_SRC varchar,
      CITY_RENTAL_ZONE varchar,
      TECHNICAL_INCOME_CHANNEL varchar
    )
  ;
  COPY temp1
    FROM '${folder}OPENDATA_BOOKING_CALL_A_BIKE.csv'
    WITH DELIMITER AS E';' NULL AS '' csv HEADER
  ;
  DROP TABLE IF EXISTS temp2;
  CREATE TABLE temp2 AS
  SELECT
    lower(city_rental_zone) AS city,
    trips.VEHICLE_HAL_ID AS key,
    trips.DATE_FROM AS started_at,
    trips.DATE_UNTIL AS ended_at,
    trips.START_RENTAL_ZONE_HAL_ID::integer AS start_station_id,
    trips.START_RENTAL_ZONE AS start_station_name,
    stations.LATITUDE AS latitude_start,
    stations.LONGITUDE AS longitude_start,
    trips.END_RENTAL_ZONE_HAL_ID::integer AS end_station_id,
    trips.END_RENTAL_ZONE AS end_station_name,
    trips.COMPUTE_EXTRA_BOOKING_FEE AS price
  FROM temp1 trips
  LEFT OUTER JOIN
    (
      SELECT * FROM bikesharing.stations WHERE provider='call_a_bike'
    ) stations
    ON
    (
      trips.START_RENTAL_ZONE_HAL_ID=stations.station_id
    )
  ;
  DROP TABLE IF EXISTS temp3;
  CREATE TABLE temp3 AS
  SELECT
    trips.city,
    trips.key,
    trips.started_at,
    trips.ended_at,
    trips.start_station_id,
    trips.latitude_start,
    trips.longitude_start,
    trips.END_RENTAL_ZONE_HAL_ID::integer AS end_station_id,
    stations.LATITUDE AS latitude_end,
    stations.LONGITUDE AS longitude_end,
    TRUE AS stationary,
    trips.price
  FROM temp2 trips
  LEFT OUTER JOIN
    (
      SELECT * FROM bikesharing.stations WHERE provider='call_a_bike'
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
      'call_a_bike' AS provider,
      city,
      key,
      started_at,
      ended_at,
      start_station_id,
      latitude_start,
      longitude_start,
      end_station_id,
      latitude_end,
      longitude_end,
      stationary,
      price,
      'bike' AS vehicle_type,
      TRUE AS from_movements
    FROM temp3
  ;
EOF
