#!/bin/bash

# this bash file creates donuts around points by substracting inner circles from outer circles
#TODO: integrate section under comment1


#__________________________________________________________________________________________
#SETTINGS START
user="user"
db="-p 5432 -d shared_mobility"
folder="/home/${user}Bikesharing/data_import/innoz/data/"

buffer_radius_steps=100 # set radius steps in m to create donut shaped containers around POIs
min_buffer_radius=$buffer_radius_steps
max_buffer_radius=800 # set maximum radius in m around POIs to be analysed

SRID=32632 # set SRID to use
city=berlin # select city to conduct analysis

poi_table=pt_stations_berlin
#SETTINGS END______________________________________________________________________________


#__________________________________________________________________________________________
# IMPORT POI TABLE
echo "*** clear database from old output table"
psql $db -c "DROP TABLE IF EXISTS ${poi_table};" # clear database from old output table
echo "*** IMPORT POI TABLE"
shp2pgsql -s '${SRID}' ${folder}${poi_table}_EPSG${SRID}.shp ${poi_table} public > ${folder}${poi_table}.sql
psql $db -f ${folder}${poi_table}.sql

# SET SRID FOR TABLES
echo "*** SET SRID FOR TABLES"
psql $db -c "ALTER TABLE ${poi_table} ADD COLUMN geom_4326 geometry(point, 4326);"
psql $db -c "UPDATE ${poi_table} SET geom_4326=st_transform(st_setsrid(geom, ${SRID}), 4326);"
#__________________________________________________________________________________________


#__________________________________________________________________________________________
#CREATE TABLE WITH POI BUFFERS WITH SELECTED STEP WIDTH
echo "*** clear database from old output table"
psql $db -c "DROP TABLE IF EXISTS poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps;" # clear database from old output table
echo "*** clear database from old temp table"
psql $db -c "DROP TABLE IF EXISTS temp;" # clear database from old temp table
echo "*** initiate temp table for the for loop"
psql $db -c "CREATE TABLE temp AS SELECT * FROM ${poi_table};" # initiate temp table for the for loop

#create geometries of the 'donut' containers in for loop
echo "create geometries of the 'donut' containers"
for ((buffer_radius=${min_buffer_radius}; buffer_radius<=${max_buffer_radius}; buffer_radius+=$buffer_radius_steps))
	do
	echo "do that for radius_$buffer_radius"
	lag_radius=$((buffer_radius - buffer_radius_steps)) # calculate radius of last iteration
	psql $db -c "CREATE TABLE temp_$buffer_radius AS SELECT *, st_transform(st_donut(st_transform(geom_4326, 32632), $buffer_radius, $lag_radius), 4326)::geometry(polygon, 4326) AS radius_$buffer_radius FROM temp;" # create geometries of the 'donut' containers
	psql $db -c "DROP TABLE IF EXISTS temp; CREATE TABLE temp AS SELECT * FROM temp_$buffer_radius;"

done

psql $db -c "CREATE TABLE poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps AS SELECT * FROM temp_$max_buffer_radius;"

for ((buffer_radius=${min_buffer_radius}; buffer_radius<=${max_buffer_radius}; buffer_radius+=$buffer_radius_steps))
	do
	echo "clear database from temporary files for radius_$buffer_radius"
	psql $db -c "DROP TABLE IF EXISTS temp_$buffer_radius;" # clear database from temporary files
done
psql $db -c "DROP TABLE IF EXISTS temp;" # clear database from temporary files


# create gist indexes for POI donuts
echo "*** create gist indexes for donuts ***"
for ((buffer_radius=${min_buffer_radius}; buffer_radius<=${max_buffer_radius}; buffer_radius+=$buffer_radius_steps))
	do
	echo "do that for radius_$buffer_radius"
	psql $db -c "DROP INDEX gist_donut_${buffer_radius};"
	psql $db -c "CREATE INDEX gist_donut_${buffer_radius} ON buffers_for_stations_${buffer_radius_steps} USING gist(radius_${buffer_radius});"
done


# create carsharing statistics for POI donuts
echo "*** create carsharing statistics for POI donuts ***"
psql $db -c "DROP TABLE IF EXISTS temp;" # clear database from old temp table
psql $db -c "CREATE TABLE temp AS SELECT * FROM poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps;"



for ((buffer_radius=${min_buffer_radius}; buffer_radius<=${max_buffer_radius}; buffer_radius+=$buffer_radius_steps))
	do
	echo "do that for radius_$buffer_radius"
	psql $db -c "CREATE TABLE temp1_${buffer_radius} AS SELECT temp.gid, count(fcs.*)::numeric AS starts_${buffer_radius} FROM temp, fcs_data.${city}_short fcs WHERE st_within(st_transform(fcs.geom_start, 4326), temp.radius_${buffer_radius}) GROUP BY temp.radius_${buffer_radius}, temp.gid;" # create geometries of the 'donut' containers
	psql $db -c "CREATE TABLE temp2_${buffer_radius} AS SELECT temp.*, temp1_${buffer_radius}.starts_${buffer_radius} FROM temp LEFT OUTER JOIN temp1_${buffer_radius} ON (temp.gid=temp1_${buffer_radius}.gid);"
	psql $db -c "ALTER TABLE temp2_${buffer_radius} ADD starts_density_${buffer_radius} numeric;"
	psql $db -c "UPDATE temp2_${buffer_radius} SET starts_density_${buffer_radius}=starts_${buffer_radius}/st_area(st_transform(radius_${buffer_radius}, 32632))::numeric;"
	psql $db -c "DROP TABLE IF EXISTS temp;" # clear database from old temp table
	psql $db -c "CREATE TABLE temp AS SELECT * FROM temp2_${buffer_radius};"
done

psql $db -c "DROP TABLE IF EXISTS poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps;" # clear database from old output table
psql $db -c "CREATE TABLE poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps AS SELECT * FROM temp;"

for ((buffer_radius=${min_buffer_radius}; buffer_radius<=${max_buffer_radius}; buffer_radius+=$buffer_radius_steps))
	do
	psql $db -c "DROP TABLE IF EXISTS temp1_$buffer_radius;"
	psql $db -c "DROP TABLE IF EXISTS temp2_$buffer_radius;" # clear database from temporary files
done
psql $db -c "DROP TABLE IF EXISTS temp;" # clear database from temporary files

echo "*** export csv-table for POI donuts "
psql $db -c "COPY (SELECT * FROM poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps) TO '/home/bbock/FCS_DATA/OUTPUT/CSV/poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps.csv' DELIMITER ';' CSV HEADER;"

#__________________________________________________________________________________________
echo "*** rearrange tables to make import into QGIS easier ***"
psql $db <<EFF
DROP TABLE IF EXISTS poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps_rearranged;
CREATE TABLE poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps_rearranged
(
  gid integer,
  geom geometry(Point),
  geom_4326 geometry(Point,4326),
  buffer_radius numeric,
  geom_radius geometry(Polygon,4326),
	starts numeric,
  starts_density numeric
  );
EFF

for ((buffer_radius=${min_buffer_radius}; buffer_radius<=${max_buffer_radius}; buffer_radius+=$buffer_radius_steps))
	do
psql $db <<EFF
INSERT INTO poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps_rearranged
	(
		gid,
	  geom,
	  geom_4326,
	  buffer_radius,
	  geom_radius,
	  starts,
	  starts_density
	)
SELECT
	gid,
  geom,
  geom_4326,
  ${buffer_radius}::numeric as buffer_radius,
  radius_${buffer_radius},
  starts_${buffer_radius},
  starts_density_${buffer_radius}
  FROM  poi_buffers_for_${poi_table}_with_${buffer_radius_steps}m_steps
;
EFF
done
#__________________________________________________________________________________________

<<COMMENT1
DROP TABLE IF EXISTS poi_buffers_for_bahnhofberlin_2008_with_100m_steps_2;
CREATE TABLE poi_buffers_for_bahnhofberlin_2008_with_100m_steps_2
AS SELECT
name,
geom_4326,
(starts_100 + starts_200 +  starts_300+  starts_400+  starts_500+  starts_600+  starts_700+  starts_800) as abs_sum,
starts_density_100*100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_100,
starts_density_200* 100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800)as starts_density_200,
starts_density_300* 100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_300,
starts_density_400* 100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_400,
starts_density_500* 100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_500,
starts_density_600* 100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_600,
starts_density_700 * 100/(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_700,
starts_density_800*100/ (starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as starts_density_800,
(starts_density_100   +starts_density_200 +  starts_density_300 +  starts_density_400 +  starts_density_500 +  starts_density_600 + starts_density_700  + starts_density_800) as rel_sum
FROM poi_buffers_for_bahnhofberlin_2008_with_100m_steps WHERE (starts_100 + starts_200 +  starts_300+  starts_400+  starts_500+  starts_600+  starts_700+  starts_800) > 0;;
COMMENT1
