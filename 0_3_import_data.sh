#!/bin/bash

db="-p 5432 -d BS"
user="bbock"

#create empty table
# "berlin" "boston" "chicago" "newyorkcity" "stuttgart" "washingtondc"

psql $db -c "DROP TABLE IF EXISTS input.movements;"
psql $db -c "CREATE SEQUENCE movements_id_seq;"
psql $db -c "CREATE TABLE input.movements(id INTEGER DEFAULT NEXTVAL('movements_id_seq'), provider varchar, city varchar, duration integer, started_at timestamp, ended_at timestamp, start_station_id integer, latitude_start double precision, longitude_start double precision, end_station_id integer, latitude_end double precision, longitude_end double precision, bike varchar, usertype varchar, birth_year integer, gender integer);"


#import data for Chicago bikesharing https://www.divvybikes.com/datachallenge

#create stations table for chicago
#import stations from csv table as can be found under the link above
psql $db -c "DROP TABLE IF EXISTS input.stations_chicago; CREATE TABLE input.stations_chicago(terminalName integer, name varchar, latitude double precision, longitude double precision, nbDocks integer, landmark varchar, installDate varchar); COPY input.stations_chicago FROM '/home/$user/BS/chicago/Divvy_Stations_2013.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"

psql $db -c "DROP TABLE IF EXISTS input.temp_chicago; CREATE TABLE input.temp_chicago(movement_id integer, started_at timestamp, ended_at timestamp, bike varchar, duration integer, start_station_id integer, start_station_name varchar, end_station_id integer, end_station_name varchar, usertype varchar,  gender varchar, birth_year varchar); COPY input.temp_chicago FROM '/home/$user/BS/chicago/Divvy_Trips_2013.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"

psql $db <<EOF
DROP TABLE IF EXISTS input.temp_2_chicago; 
CREATE TABLE input.temp_2_chicago AS SELECT
trips.started_at,
trips.ended_at,
trips.start_station_id, 
stations.latitude AS latitude_start, 
stations.longitude AS longitude_start, 
trips.end_station_id,
trips.bike, 
trips.usertype,
(CASE WHEN trips.birth_year='NULL' THEN NULL ELSE CASE WHEN length(trips.birth_year)=4 THEN (trips.birth_year)::integer ELSE NULL END END)::integer birth_year,
(CASE WHEN trips.gender='Male' THEN 1 ELSE CASE WHEN trips.gender='Female' THEN 2 ELSE 0 END END)::integer gender
FROM input.temp_chicago trips LEFT OUTER JOIN input.stations_chicago stations ON (trips.start_station_id=stations.terminalName);

DROP TABLE IF EXISTS input.temp_3_chicago; 
CREATE TABLE input.temp_3_chicago AS SELECT
date_part('seconds', trips.ended_at - trips.started_at) AS duration,
trips.started_at,
trips.ended_at,
trips.start_station_id, 
trips.latitude_start, 
trips.longitude_start,
stations.terminalname AS end_station_id, 
stations.latitude AS latitude_end, 
stations.longitude AS longitude_end, 
trips.bike, 
trips.usertype,
trips.birth_year,
trips.gender
FROM input.temp_2_chicago trips LEFT OUTER JOIN input.stations_chicago stations ON (trips.end_station_id=stations.terminalName);

INSERT INTO input.movements(provider, city, duration, started_at, ended_at, start_station_id, latitude_start, longitude_start, end_station_id, latitude_end, longitude_end, bike, usertype, birth_year, gender) SELECT 'Divvy' AS provider, 'chicago' AS city, * FROM input.temp_3_chicago;
EOF
psql $db -c "DROP TABLE IF EXISTS input.temp_chicago;"
psql $db -c "DROP TABLE IF EXISTS input.temp_2_chicago;"
psql $db -c "DROP TABLE IF EXISTS input.temp_3_chicago;"



#import data for newyork bikesharing http://www.citibikenyc.com/system-data

for year in {2013..2014}
do

for month in {01..12}
do

psql $db -c "DROP TABLE IF EXISTS input.temp_newyorkcity_${year}_${month}; CREATE TABLE input.temp_newyorkcity_${year}_${month}(duration integer, started_at timestamp, ended_at timestamp, start_station_id integer, start_station_name varchar, latitude_start double precision, longitude_start double precision, end_station_id integer, end_station_name varchar, latitude_end double precision, longitude_end double precision, bike varchar, usertype varchar, birth_year integer, gender integer); COPY input.temp_newyorkcity_${year}_${month} FROM '/home/$user/BS/newyork/${year}-${month}_nyc_bikesharing.csv' WITH DELIMITER AS E',' NULL AS '\N' csv HEADER;"
psql $db -c "ALTER TABLE input.temp_newyorkcity_${year}_${month} DROP COLUMN start_station_name, DROP COLUMN end_station_name;"
psql $db -c "INSERT INTO input.movements(provider, city, duration, started_at, ended_at, start_station_id, latitude_start, longitude_start, end_station_id, latitude_end, longitude_end, bike, usertype, birth_year, gender) SELECT 'citybike' AS provider, 'newyorkcity' AS city, * FROM input.temp_newyorkcity_${year}_${month};"
psql $db -c "DROP TABLE IF EXISTS input.temp_newyorkcity_${year}_${month};"


done
done

#import data for washingtondc bikesharing https://www.capitalbikeshare.com/system-data
#create stations table for washington
#import stations from csv table wich was generated from XML-file
psql $db -c "DROP TABLE IF EXISTS input.stations_washingtondc; CREATE TABLE input.stations_washingtondc(id integer, name varchar, terminalName integer, lastCommWithServer bigint, latitude double precision, longitude double precision, installed boolean, locked boolean, installDate bigint, removalDate  bigint, temporary boolean, public boolean, nbBikes integer, nbEmptyDocks integer, latestUpdateTime bigint, stations_Id integer); COPY input.stations_washingtondc FROM '/home/$user/BS/washingtondc/station.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"

#import trips for washington (for seperate time periods because of inconsistent input data)
#TODO: check if ON (left(trips.start_station_name, length(trips.start_station_name)-8) = stations.name) can be optimised
for year in {2010..2011}
do
for quarter in 1st 2nd 3rd 4th
do
psql $db -c "DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter}; CREATE TABLE input.temp_washingtondc_${year}_${quarter}(duration varchar, started_at varchar, ended_at varchar, start_station_name varchar,  end_station_name varchar, bike varchar, usertype varchar); COPY input.temp_washingtondc_${year}_${quarter} FROM '/home/$user/BS/washingtondc/${year}-${quarter}-quarter.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"
psql $db <<EOF
CREATE TABLE input.temp_2_washingtondc_${year}_${quarter} AS SELECT
to_timestamp(trips.started_at, 'MM/DD/YYYY HH24:MI') AS started_at,
to_timestamp(trips.ended_at, 'MM/DD/YYYY HH24:MI') AS ended_at,
trips.start_station_name,
stations.terminalname AS start_station_id, 
stations.latitude AS latitude_start, 
stations.longitude AS longitude_start, 
trips.end_station_name, 
trips.bike, 
trips.usertype
FROM input.temp_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (left(trips.start_station_name, length(trips.start_station_name)-8) = stations.name);


CREATE TABLE input.temp_3_washingtondc_${year}_${quarter} AS SELECT
date_part('seconds', trips.ended_at - trips.started_at) AS duration,
trips.started_at,
trips.ended_at,
trips.start_station_id, 
trips.latitude_start, 
trips.longitude_start,
stations.terminalname AS end_station_id, 
stations.latitude AS latitude_end, 
stations.longitude AS longitude_end, 
trips.bike, 
trips.usertype
FROM input.temp_2_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (left(trips.end_station_name, length(trips.end_station_name)-8) = stations.name);

INSERT INTO input.movements(provider, city, duration, started_at, ended_at, start_station_id, latitude_start, longitude_start, end_station_id, latitude_end, longitude_end, bike, usertype, birth_year, gender) SELECT 'capitalbikeshare' AS provider, 'washingtondc' AS city, *, NULL AS birth_year, NULL AS gender FROM input.temp_3_washingtondc_${year}_${quarter};

DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_2_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_3_washingtondc_${year}_${quarter};

EOF
done
done

year="2012"
for quarter in 1st 2nd
do
psql $db -c "DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter}; CREATE TABLE input.temp_washingtondc_${year}_${quarter}(duration varchar, durationInSeconds numeric, started_at varchar, start_station_name varchar, start_terminalName integer,  ended_at varchar,  end_station_name varchar, end_terminalName integer, bike varchar, usertype varchar); COPY input.temp_washingtondc_${year}_${quarter} FROM '/home/$user/BS/washingtondc/${year}-${quarter}-quarter.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"
psql $db <<EOF
CREATE TABLE input.temp_2_washingtondc_${year}_${quarter} AS SELECT
to_timestamp(trips.started_at, 'MM/DD/YYYY HH24:MI') AS started_at,
to_timestamp(trips.ended_at, 'MM/DD/YYYY HH24:MI') AS ended_at,
trips.start_station_name,
stations.terminalname AS start_station_id, 
stations.latitude AS latitude_start, 
stations.longitude AS longitude_start, 
trips.end_terminalName, 
trips.bike, 
trips.usertype
FROM input.temp_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (trips.start_terminalName = stations.terminalname);


CREATE TABLE input.temp_3_washingtondc_${year}_${quarter} AS SELECT
date_part('seconds', trips.ended_at - trips.started_at) AS duration,
trips.started_at,
trips.ended_at,
trips.start_station_id, 
trips.latitude_start, 
trips.longitude_start,
stations.terminalname AS end_station_id, 
stations.latitude AS latitude_end, 
stations.longitude AS longitude_end, 
trips.bike, 
trips.usertype
FROM input.temp_2_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (trips.end_terminalName = stations.terminalname);

INSERT INTO input.movements(provider, city, duration, started_at, ended_at, start_station_id, latitude_start, longitude_start, end_station_id, latitude_end, longitude_end, bike, usertype, birth_year, gender) SELECT 'capitalbikeshare' AS provider, 'washingtondc' AS city,*, NULL AS birth_year, NULL AS gender FROM input.temp_3_washingtondc_${year}_${quarter};

DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_2_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_3_washingtondc_${year}_${quarter};
EOF
done
for quarter in 3rd 4th
do
psql $db -c "DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter}; CREATE TABLE input.temp_washingtondc_${year}_${quarter}(duration varchar, started_at varchar, start_station_name varchar, start_terminalName integer,  ended_at varchar,  end_station_name varchar, end_terminalName integer, bike varchar, usertype varchar); COPY input.temp_washingtondc_${year}_${quarter} FROM '/home/$user/BS/washingtondc/${year}-${quarter}-quarter.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"
psql $db <<EOF
CREATE TABLE input.temp_2_washingtondc_${year}_${quarter} AS SELECT
to_timestamp(trips.started_at, 'MM/DD/YYYY HH24:MI') AS started_at,
to_timestamp(trips.ended_at, 'MM/DD/YYYY HH24:MI') AS ended_at,
trips.start_station_name,
stations.terminalname AS start_station_id, 
stations.latitude AS latitude_start, 
stations.longitude AS longitude_start, 
trips.end_terminalName, 
trips.bike, 
trips.usertype
FROM input.temp_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (trips.start_terminalName = stations.terminalname);


CREATE TABLE input.temp_3_washingtondc_${year}_${quarter} AS SELECT
date_part('seconds', trips.ended_at - trips.started_at) AS duration,
trips.started_at,
trips.ended_at,
trips.start_station_id, 
trips.latitude_start, 
trips.longitude_start,
stations.terminalname AS end_station_id, 
stations.latitude AS latitude_end, 
stations.longitude AS longitude_end, 
trips.bike, 
trips.usertype
FROM input.temp_2_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (trips.end_terminalName = stations.terminalname);

INSERT INTO input.movements(provider, city, duration, started_at, ended_at, start_station_id, latitude_start, longitude_start, end_station_id, latitude_end, longitude_end, bike, usertype, birth_year, gender) SELECT 'capitalbikeshare' AS provider, 'washingtondc' AS city,*, NULL AS birth_year, NULL AS gender FROM input.temp_3_washingtondc_${year}_${quarter};

DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_2_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_3_washingtondc_${year}_${quarter};
EOF
done

for year in {2013..2014}
do
for quarter in 1st 2nd 3rd 4th
do
psql $db -c "DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter}; CREATE TABLE input.temp_washingtondc_${year}_${quarter}(duration varchar, started_at varchar, start_station_name varchar, start_terminalName integer,  ended_at varchar,  end_station_name varchar, end_terminalName integer, bike varchar, usertype varchar); COPY input.temp_washingtondc_${year}_${quarter} FROM '/home/$user/BS/washingtondc/${year}-${quarter}-quarter.csv' WITH DELIMITER AS E',' NULL AS 'NULL' csv HEADER;"
psql $db <<EOF
CREATE TABLE input.temp_2_washingtondc_${year}_${quarter} AS SELECT
to_timestamp(trips.started_at, 'MM/DD/YYYY HH24:MI') AS started_at,
to_timestamp(trips.ended_at, 'MM/DD/YYYY HH24:MI') AS ended_at,
trips.start_station_name,
stations.terminalname AS start_station_id, 
stations.latitude AS latitude_start, 
stations.longitude AS longitude_start, 
trips.end_terminalName, 
trips.bike, 
trips.usertype
FROM input.temp_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (trips.start_terminalName = stations.terminalname);


CREATE TABLE input.temp_3_washingtondc_${year}_${quarter} AS SELECT
date_part('seconds', trips.ended_at - trips.started_at) AS duration,
trips.started_at,
trips.ended_at,
trips.start_station_id, 
trips.latitude_start, 
trips.longitude_start,
stations.terminalname AS end_station_id, 
stations.latitude AS latitude_end, 
stations.longitude AS longitude_end, 
trips.bike, 
trips.usertype
FROM input.temp_2_washingtondc_${year}_${quarter} trips LEFT OUTER JOIN input.stations_washingtondc stations ON (trips.end_terminalName = stations.terminalname);

INSERT INTO input.movements(provider, city, duration, started_at, ended_at, start_station_id, latitude_start, longitude_start, end_station_id, latitude_end, longitude_end, bike, usertype, birth_year, gender) SELECT 'capitalbikeshare' AS provider, 'washingtondc' AS city,*, NULL AS birth_year, NULL AS gender FROM input.temp_3_washingtondc_${year}_${quarter};

DROP TABLE IF EXISTS input.temp_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_2_washingtondc_${year}_${quarter};
DROP TABLE IF EXISTS input.temp_3_washingtondc_${year}_${quarter};
EOF
done
done



echo "*** ********** ***"
echo "*** GESCHAFFT! ***"
echo "*** ********** ***"
