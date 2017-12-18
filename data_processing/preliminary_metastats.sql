-- calculate vehicle counts, sightings and movement rates for each city and provider
DROP TABLE IF EXISTS bikesharing.providers;
CREATE TABLE bikesharing.providers AS
SELECT provider, city, 'sightings' AS source, date_trunc('day' , min(first_seen_at)) AS min, date_trunc('day' , max(last_seen_at)) AS max, count(*) AS recordings
FROM bikesharing.vehicle_sightings
GROUP BY provider, city, date_trunc('day' , min(first_seen_at)), date_trunc('day' , max(last_seen_at))
ORDER BY source ASC, city ASC, provider ASC;
