-- this table will update station movements, so that keys from 
-- 2015 ('nextbike germany') are translated to keys from 2016 ('nextbike-dusseldorf')

--DELETE TABLE IF EXISTS key_table_nextbike;

CREATE TABLE key_table_nextbike AS
WITH stations_temp AS (
SELECT
round(latitude, 4) AS latitude,
round(longitude, 4) AS longitude,
CASE WHEN provider='nextbike-dusseldorf' THEN key ELSE NULL END AS key_2016,
CASE WHEN provider='nextbike germany' THEN key ELSE NULL END AS key_2015,
geom
FROM duesseldorf.stations
WHERE provider IN ('nextbike-dusseldorf', 'nextbike germany')
)
SELECT
latitude,
longitude,
min(geom) AS geom,
min(key_2016) AS key_2016,
min(key_2015) AS key_2015,
count(*)
FROM stations_temp
GROUP BY latitude, longitude;

DELETE FROM key_table_nextbike WHERE count=1 OR key_2016 IS NULL OR  key_2015 IS NULL;

UPDATE duesseldorf.station_movements AS station_movements
SET key = key_table_nextbike.key_2016
FROM key_table_nextbike
WHERE key_table_nextbike.key_2015 = station_movements.key;

UPDATE duesseldorf.station_occupancies AS station_occupancies
SET key = key_table_nextbike.key_2016
FROM key_table_nextbike
WHERE key_table_nextbike.key_2015 = station_occupancies.key;
