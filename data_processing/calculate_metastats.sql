-- GET META STATS

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
		FROM bikesharing.vehicle_sightings_year) t1
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
		FROM bikesharing.vehicle_sightings_year) t1
	GROUP BY crowflydistance_percentiles
	ORDER BY crowflydistance_percentiles ASC;
