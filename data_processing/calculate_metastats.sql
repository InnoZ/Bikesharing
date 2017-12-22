-- GET META STATS
-- calculate vehicle counts, sightings and movement rates for each city and provider
DROP TABLE IF EXISTS bikesharing.providers;
CREATE TABLE bikesharing.providers AS
SELECT
	provider varchar,
	city varchar,
	source varchar,
	min timestamp,
	max timestamp,
	recordings integer
INSERT INTO bikesharing.providers
	(
		provider,
		city,
		source,
		min,
		max,
		recordings
	)
SELECT
	provider,
	city,
	'sightings' AS source,
	date_trunc('day' , min(first_seen_at)) AS min,
	date_trunc('day' , max(last_seen_at)) AS max,
	count(*) AS recordings
FROM bikesharing.vehicle_sightings
GROUP BY provider, city, date_trunc('day' , min(first_seen_at)), date_trunc('day' , max(last_seen_at))
ORDER BY source ASC, city ASC, provider ASC;
INSERT INTO bikesharing.providers
	(
		provider,
		city,
		source,
		min,
		max,
		recordings
	)
SELECT
	provider,
	city,
	'movements' AS source,
	date_trunc('day' , min(first_seen_at)) AS min,
	date_trunc('day' , max(last_seen_at)) AS max,
	count(*) AS recordings
FROM bikesharing.vehicle_movements
GROUP BY provider, city, date_trunc('day' , min(first_seen_at)), date_trunc('day' , max(last_seen_at))
ORDER BY source ASC, city ASC, provider ASC;
INSERT INTO bikesharing.providers
	(
		provider,
		city,
		source,
		min,
		max,
		recordings
	)
SELECT
	provider,
	city,
	'stations' AS source,
	date_trunc('day' , min(first_seen_at)) AS min,
	date_trunc('day' , max(last_seen_at)) AS max,
	count(*) AS recordings
FROM bikesharing.station_movements
GROUP BY provider, city, date_trunc('day' , min(first_seen_at)), date_trunc('day' , max(last_seen_at))
ORDER BY source ASC, city ASC, provider ASC;

-- calculate boxplot for movement durations
DROP TABLE IF EXISTS bikesharing.duration_percentiles;
CREATE TABLE bikesharing.duration_percentiles AS
	SELECT
	 	count(*),
		min(duration),
		median(duration),
		max(duration),
		duration_percentiles
	FROM
		(SELECT
			duration,
			ntile(100) over (order by duration) AS duration_percentiles
		FROM bikesharing.vehicle_movements_extended) t1
	GROUP BY duration_percentiles
	ORDER BY duration_percentiles ASC;

-- calculate boxplot for movement crowflydistance
DROP TABLE IF EXISTS bikesharing.crowflydistance_percentiles;
CREATE TABLE bikesharing.crowflydistance_percentiles AS
	SELECT
		count(*),
		min(crowflydistance),
		avg(crowflydistance),
		max(crowflydistance),
		crowflydistance_percentiles
	FROM
		(SELECT
			crowflydistance,
			ntile(100) over (order by crowflydistance) AS crowflydistance_percentiles
		FROM bikesharing.vehicle_movements_extended) t1
	GROUP BY crowflydistance_percentiles
	ORDER BY crowflydistance_percentiles ASC;
