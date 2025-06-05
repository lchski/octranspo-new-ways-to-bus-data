-- First, ensure your database is set up using `load.sql`
-- Then, to run the commands in this file, you can use: `.read output-for-web.sql`

COPY stops_normalized TO 'data/out/for-web/stops_normalized.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');

CREATE TEMP TABLE web_stop_times_by_stop AS (
	SELECT
		st.source, st.service_id, st.service_window, st.stop_code, s.ward_number, count(st.*) as n_stop_times
	FROM (SELECT DISTINCT(*) FROM stop_times) st
	LEFT JOIN stops_normalized s USING (stop_code)
	GROUP BY ALL
	ORDER BY source, service_id, service_window, stop_code
);

UPDATE web_stop_times_by_stop
	SET source = 'current'
	WHERE source = 'legacy';

UPDATE web_stop_times_by_stop
	SET source = 'new'
	WHERE source = 'nwtb';

COPY web_stop_times_by_stop TO 'data/out/for-web/stop_times_by_stop.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');

CREATE TEMP TABLE web_stop_times AS (
	WITH stop_times_with_arrivals AS (
		SELECT
			source, service_id, service_window, regexp_replace(t.route_id, '-(?:350|354)$', '') AS route_id, t.direction_id, t.trip_headsign, stop_times.stop_code, stops_normalized.stop_lat_normalized, stops_normalized.stop_lon_normalized, stops_normalized.ward_number,
			add(add(
				arrival_time[0:2]::Integer * 60 * 60,
				arrival_time[4:5]::Integer * 60),
				arrival_time[7:8]::Integer
			) AS arrival_time_s
		FROM stop_times
		LEFT JOIN stops_normalized ON stop_times.stop_code = stops_normalized.stop_code
		LEFT JOIN
			(
				SELECT
					trip_id,
					regexp_replace(route_id, '-1$', '') AS route_id,
					direction_id,
					trip_headsign
				FROM trips
			) t ON stop_times.trip_id = t.trip_id
	)
	SELECT
		* EXCLUDE(arrival_time_s),
		lead(arrival_time_s) OVER (
			PARTITION BY stop_code, source, service_id, route_id, direction_id
			ORDER BY arrival_time_s
		) - arrival_time_s AS s_until_next_arrival
	FROM stop_times_with_arrivals
	ORDER BY stop_lat_normalized, stop_lon_normalized, source, service_id, arrival_time_s
);

UPDATE web_stop_times
	SET source = 'current'
	WHERE source = 'legacy';

UPDATE web_stop_times
	SET source = 'new'
	WHERE source = 'nwtb';

COPY web_stop_times TO 'data/out/for-web/stop_times.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');

CREATE TEMP TABLE web_routes AS (
	WITH trip_counts AS (
	SELECT
		source,
		regexp_replace(route_id, '-1$', '') AS route_id,
		direction_id,
		COUNT(*) AS total_trips
	FROM trips
	GROUP BY source, route_id, direction_id
	),
	cleaned_routes AS (
	SELECT
		source,
		regexp_replace(route_id, '-1$', '') AS route_id,
		direction_id,
		trip_headsign,
		COUNT(*) AS headsign_count
	FROM trips
	GROUP BY source, route_id, direction_id, trip_headsign
	),
	ranked_headsigns AS (
	SELECT
		source,
		route_id,
		direction_id,
		trip_headsign,
		headsign_count,
		ROW_NUMBER() OVER (
		PARTITION BY source, route_id, direction_id
		ORDER BY headsign_count DESC
		) AS rn
	FROM cleaned_routes
	)
	SELECT
		r.source,
		regexp_replace(r.route_id, '-(?:350|354)$', '') AS route_id,
		r.direction_id,
		r.trip_headsign AS most_common_headsign,
		t.total_trips
	FROM ranked_headsigns r
	JOIN trip_counts t ON 
		r.source = t.source AND
		r.route_id = t.route_id AND
		r.direction_id = t.direction_id
	WHERE r.rn = 1
	ORDER BY TRY_CAST(regexp_replace(r.route_id, '-(?:350|354)$', '') AS INTEGER), r.direction_id, r.source
);

UPDATE web_routes
	SET source = 'current'
	WHERE source = 'legacy';

UPDATE web_routes
	SET source = 'new'
	WHERE source = 'nwtb';

COPY web_routes TO 'data/out/for-web/routes.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');

--- To convert these files to CSV:
-- COPY (FROM read_parquet('data/out/for-web/stops_normalized.parquet')) TO 'data/out/for-web/stops_normalized.csv';
-- COPY (FROM read_parquet('data/out/for-web/stop_times_by_stop.parquet')) TO 'data/out/for-web/stop_times_by_stop.csv';
-- COPY (FROM read_parquet('data/out/for-web/stop_times.parquet')) TO 'data/out/for-web/stop_times.csv';
-- COPY (FROM read_parquet('data/out/for-web/routes.parquet')) TO 'data/out/for-web/routes.csv';
