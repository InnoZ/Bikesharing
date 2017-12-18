DROP TABLE IF EXISTS duesseldorf.stations;
CREATE TABLE duesseldorf.stations AS SELECT provider, key, latitude, longitude, capacity, vehicle_type FROM duesseldorf.station_occupancies GROUP BY provider, key, latitude, longitude, capacity, vehicle_type;
