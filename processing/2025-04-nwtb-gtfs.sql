CREATE TABLE calendar as
	SELECT *
	FROM read_csv("data/source/octranspo-modern-gtfs/2025-04-18-GTFSExport/calendar.txt", dateformat='%Y%m%d', types={'start_date': 'DATE', 'end_date': 'DATE'});
CREATE TABLE calendar_dates as
	SELECT *
	FROM read_csv("data/source/octranspo-modern-gtfs/2025-04-18-GTFSExport/calendar_dates.txt", dateformat='%Y%m%d', types={'date': 'DATE'}); -- TODO: filter to just the dates we're interested in?

CREATE TABLE stops as
	SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon, platform_code
	FROM read_csv("data/source/octranspo-modern-gtfs/2025-04-18-GTFSExport/stops.txt");
CREATE TABLE stop_times as
	SELECT trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type -- may not need departure_time, pickup_type, drop_off_type
	FROM read_csv("data/source/octranspo-modern-gtfs/2025-04-18-GTFSExport/stop_times.txt", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});



-- Dates (Fri/Sat/Sun)
-- - Legacy: 2025-04-11, 2025-04-12, 2025-04-13
-- - NWTB: 2025-05-09, 2025-05-10, 2025-05-11

---- TODO: maybe do this differently, so we can still keep the full set of trips, then separate into trips_unused as we used to—and, manually add back the few exceptional weekday trips for, e.g., school / shopping

CREATE TEMPORARY TABLE service_ids_oi_raw AS
	SELECT service_id, friday, saturday, sunday, start_date, end_date
	FROM calendar
	WHERE
		(friday = 1 OR saturday = 1 OR sunday = 1) AND
		(
			(start_date <= '2025-04-11'::DATE AND end_date >= '2025-04-11'::DATE) OR
			(start_date <= '2025-04-12'::DATE AND end_date >= '2025-04-12'::DATE) OR
			(start_date <= '2025-04-13'::DATE AND end_date >= '2025-04-13'::DATE) OR
			(start_date <= '2025-05-09'::DATE AND end_date >= '2025-05-09'::DATE) OR
			(start_date <= '2025-05-10'::DATE AND end_date >= '2025-05-10'::DATE) OR
			(start_date <= '2025-05-11'::DATE AND end_date >= '2025-05-11'::DATE)
		);

ALTER TABLE service_ids_oi_raw ADD COLUMN day_of_week VARCHAR;
ALTER TABLE service_ids_oi_raw ADD COLUMN source VARCHAR;

UPDATE service_ids_oi_raw
	SET day_of_week = CASE
		WHEN friday = 1 THEN 'weekday'
		WHEN saturday = 1 THEN 'saturday'
		WHEN sunday = 1 THEN 'sunday'
		ELSE 'mixed'
	END;

UPDATE service_ids_oi_raw
	SET source = CASE
		WHEN end_date < '2025-04-27'::DATE THEN 'legacy'
		ELSE 'nwtb'
	END;

CREATE TEMPORARY TABLE service_ids_oi_annotated AS
	SELECT service_id, day_of_week, source
	FROM service_ids_oi_raw;

CREATE TEMPORARY TABLE calendar_dates_oi AS
	SELECT *
	FROM calendar_dates
		WHERE
			date in ('2025-04-11'::DATE, '2025-04-12'::DATE, '2025-04-13'::DATE, '2025-05-09'::DATE, '2025-05-10'::DATE, '2025-05-11'::DATE);

ALTER TABLE calendar_dates_oi ADD COLUMN day_of_week VARCHAR;
ALTER TABLE calendar_dates_oi ADD COLUMN source VARCHAR;

UPDATE calendar_dates_oi
	SET day_of_week = CASE
		WHEN date = '2025-04-11'::DATE THEN 'weekday'
		WHEN date = '2025-04-12'::DATE THEN 'saturday'
		WHEN date = '2025-04-13'::DATE THEN 'sunday'
		WHEN date = '2025-05-09'::DATE THEN 'weekday'
		WHEN date = '2025-05-10'::DATE THEN 'saturday'
		WHEN date = '2025-05-11'::DATE THEN 'sunday'
	END;

UPDATE calendar_dates_oi
	SET source = CASE
		WHEN date < '2025-04-27'::DATE THEN 'legacy'
		ELSE 'nwtb'
	END;

INSERT INTO
	service_ids_oi_annotated (service_id, day_of_week, source)
	SELECT service_id, day_of_week, source FROM calendar_dates_oi WHERE exception_type = 1;

CREATE TABLE service_ids_oi AS
	(
		SELECT DISTINCT service_id, day_of_week, source
		FROM (
			SELECT sioia.*, cdoi.exception_type
				FROM service_ids_oi_annotated sioia
				LEFT JOIN calendar_dates_oi cdoi ON sioia.service_id = cdoi.service_id
		)
		WHERE
			exception_type != 2 OR
			exception_type IS null
		ORDER BY source, day_of_week, service_id
	);

CREATE TEMPORARY TABLE trips_raw as
	SELECT route_id, service_id, trip_id, trip_headsign, direction_id -- trip_headsign probably not useful? but maybe if we're trying to join / infer directions later
	FROM read_csv("data/source/octranspo-modern-gtfs/2025-04-18-GTFSExport/trips.txt");

---- create our reference table of trips
----- note the manual addition of several trip IDs—these are for the shopper routes (https://www.octranspo.com/en/our-services/bus-o-train-network/service-types/shopper-routes/)
----- we add them manually to make sure rural stops are accurately reflected
----- we find the shopper routes with these queries to find unused stop IDs, making sure nothing else is getting caught up or missed:
-- COPY (SELECT stop_id FROM stops ANTI JOIN stop_times USING (stop_id)) TO 'data/out/tmp-unused-stop-ids.csv';
-- COPY (SELECT DISTINCT(trip_id) FROM stop_times_unused WHERE stop_id IN (FROM read_csv('data/out/tmp-unused-stop-ids.csv', types={'stop_id': 'VARCHAR'}))) TO 'data/out/tmp-dropped-trips-from-unused-stops.csv';
-- SELECT DISTINCT(route_id) FROM trips_raw WHERE trip_id IN (FROM read_csv('data/out/tmp-dropped-trips-from-unused-stops.csv')) ORDER BY route_id;
-- SELECT route_id, arrival_time, first_value(trip_id) FROM (SELECT t.route_id, t.trip_headsign, st.* FROM stop_times st LEFT JOIN trips_raw t USING (trip_id) WHERE trip_id IN (FROM read_csv('data/out/tmp-dropped-trips-from-unused-stops.csv')) AND stop_sequence = 1 ORDER BY route_id, arrival_time, trip_id) GROUP BY route_id, arrival_time;
-- COPY (SELECT route_id, arrival_time, first(trip_id) as representative_trip_id FROM (SELECT t.route_id, t.trip_headsign, st.* FROM stop_times st LEFT JOIN trips_raw t USING (trip_id) WHERE trip_id IN (FROM read_csv('data/out/tmp-dropped-trips-from-unused-stops.csv')) AND stop_sequence = 1 ORDER BY route_id, arrival_time, trip_id) GROUP BY route_id, arrival_time ORDER BY route_id, arrival_time) TO 'data/corrections/missing_shopper_trips.csv';

CREATE TABLE trips AS
	(
		SELECT 
			t.route_id,
			sioi.day_of_week AS service_id,
			t.trip_id,
			t.trip_headsign,
			t.direction_id,
			sioi.source,
			t.service_id AS service_id_original
		FROM trips_raw t
		LEFT JOIN service_ids_oi sioi
		ON t.service_id = sioi.service_id
		WHERE
			source IS NOT NULL OR
			route_id IN ('301', '301-1', '302', '302-1', '303', '303-1', '304', '304-1') AND
			trip_id IN (SELECT representative_trip_id FROM read_csv('data/corrections/missing_shopper_trips.csv'))
	);

UPDATE trips
	SET
		service_id = 'weekday',
		source = CASE
			WHEN contains(service_id_original, '-1') THEN 'nwtb'
			ELSE 'legacy'
		END
	WHERE
		source IS NULL AND
		route_id IN ('301', '301-1', '302', '302-1', '303', '303-1', '304', '304-1') AND
		trip_id IN (SELECT representative_trip_id FROM read_csv('data/corrections/missing_shopper_trips.csv'));


---
-- add service windows
ALTER TABLE stop_times ADD COLUMN arrival_time_frac DOUBLE; --- arrival_time_frac and service_window;
ALTER TABLE stop_times ADD COLUMN service_window VARCHAR;

UPDATE stop_times
	SET arrival_time_frac = round(
	  add(
		arrival_time[0:2]::Integer,
		arrival_time[4:5]::Integer / 60
	  ), 2);

-- service window times from: https://www.octranspo.com/en/our-services/bus-o-train-network/service-types/o-train-line-1#hoursOp
UPDATE stop_times
	SET service_window = CASE
	  WHEN arrival_time_frac >= 5 AND arrival_time_frac < 6.5 THEN 'off_peak_morning'
	  WHEN arrival_time_frac >= 6.5 AND arrival_time_frac < 9 THEN 'peak_morning'
	  WHEN arrival_time_frac >= 9 AND arrival_time_frac < 15 THEN 'off_peak_midday'
	  WHEN arrival_time_frac >= 15 AND arrival_time_frac < 18.5 THEN 'peak_afternoon'
	  WHEN arrival_time_frac >= 18.5 AND arrival_time_frac < 23 THEN 'off_peak_evening'
	  ELSE 'off_peak_night'
	END;

-- join in the corrected stop_code for stops with null stop_code
UPDATE stops
	SET
		stop_code = correction_null_stop_codes.stop_code
	FROM read_csv('data/corrections/null_stop_codes.csv') correction_null_stop_codes
	WHERE
		stops.stop_id = correction_null_stop_codes.stop_id
		AND correction_null_stop_codes.source = 'nwtb-2025-04'
		AND stops.stop_code IS NULL;

---- QC: check it worked with: `from stops where stop_code is null;`


-- correct errant stop_code
UPDATE stops
	SET
		stop_code = correction_errant_stop_codes.stop_code_corrected
	FROM read_csv('data/corrections/errant_stop_codes.csv', all_varchar = true) correction_errant_stop_codes
	WHERE
		stops.stop_id = correction_errant_stop_codes.stop_id
		AND correction_errant_stop_codes.source = 'nwtb-2025-04'
		AND stops.stop_code = correction_errant_stop_codes.stop_code_current;

-- join trip ID and stop code from relevant tables
ALTER TABLE stop_times ADD COLUMN source VARCHAR;
ALTER TABLE stop_times ADD COLUMN service_id VARCHAR;
ALTER TABLE stop_times ADD COLUMN stop_code VARCHAR;

UPDATE stop_times
	SET
		source = trips.source,
		service_id = trips.service_id
	FROM trips
	WHERE stop_times.trip_id = trips.trip_id;

UPDATE stop_times
	SET stop_code = stops.stop_code
	FROM stops
	WHERE stop_times.stop_id = stops.stop_id;



-- filter down to just representative data

--- backup entries before deletion from main tables
---- if you want to see the "original", run: `FROM trips UNION FROM trips_unused;` or the same for stop_times
CREATE TABLE stop_times_unused AS
	FROM stop_times
	WHERE service_id IS NULL;

--- remove the backed-up entries
---- !! this removes ~80% of stop_times – probably makes sense, as we're cutting 4/5 of the days of the week, but this suggests Saturday/Sunday are much less service...
DELETE FROM stop_times
	WHERE service_id IS NULL;






-- NORMALIZING

--- normalize stops to draw from stop_code, not stop_id
---- TODO: this may be more than's needed now that we have just one set of stops
CREATE TEMPORARY TABLE stop_ids_normalized AS (
	SELECT stop_code, stop_id as stop_id_normalized FROM (
		WITH stop_counts AS (
			SELECT stop_id, count(*) AS n_stops FROM stop_times GROUP BY all
		)
		SELECT
			s.stop_code,
			s.stop_id,
			sc.n_stops,
			ROW_NUMBER() OVER (PARTITION BY s.stop_code ORDER BY sc.n_stops DESC) AS n_stops_rank
		FROM stops s
		JOIN stop_counts sc ON s.stop_id = sc.stop_id
	)
	WHERE n_stops_rank = 1
);

CREATE TEMPORARY TABLE stops_normalized_tmp AS (
	SELECT
		s.stop_code,
		s.stop_id,
		s.stop_name,
		s.stop_lat,
		s.stop_lon,
		s_ids.stop_id_normalized
	FROM stops s
	JOIN stop_ids_normalized s_ids ON
		s.stop_code = s_ids.stop_code
	);

ALTER TABLE stops_normalized_tmp ADD COLUMN stop_name_normalized VARCHAR;
ALTER TABLE stops_normalized_tmp ADD COLUMN stop_lat_normalized DOUBLE;
ALTER TABLE stops_normalized_tmp ADD COLUMN stop_lon_normalized DOUBLE;

UPDATE stops_normalized_tmp sn
	SET
		stop_name_normalized = s.stop_name,
		stop_lat_normalized = s.stop_lat,
		stop_lon_normalized = s.stop_lon
	FROM stops s
	WHERE
		sn.stop_id_normalized = s.stop_id;

CREATE TEMPORARY TABLE stops_normalized_tmp_distinct AS (
	SELECT DISTINCT
		stop_code,
		stop_id_normalized,
		stop_name_normalized,
		stop_lat_normalized,
		stop_lon_normalized
	FROM stops_normalized_tmp
);

DROP TABLE stop_ids_normalized;
DROP TABLE stops_normalized_tmp;

--- fix stop names for multiplatform stops
---- manually create the list of multiplatform stops based on stop codes and build a replacement list from that:
---- COPY (SELECT 'nwtb-2025-04' AS source, stop_code, stop_id_normalized, stop_name_normalized, '' AS stop_name_corrected FROM (SELECT s.stop_code, sn.stop_id_normalized, sn.stop_name_normalized, s.n FROM (SELECT stop_code, COUNT(*) AS n FROM stops GROUP BY stop_code HAVING n > 1) s JOIN stops_normalized_tmp_distinct sn ON sn.stop_code = s.stop_code ORDER BY n DESC)) TO 'data/out/tmp-multiplatform-stops.csv';
UPDATE stops_normalized_tmp_distinct sn
	SET
		stop_name_normalized = correction_multiplatform_stops.stop_name_corrected
	FROM read_csv('data/corrections/multiplatform_stop_names.csv', all_varchar = true) correction_multiplatform_stops
	WHERE
		correction_multiplatform_stops.source = 'nwtb-2025-04' AND
		sn.stop_code = correction_multiplatform_stops.stop_code AND
		sn.stop_id_normalized = correction_multiplatform_stops.stop_id_normalized AND
		sn.stop_name_normalized = correction_multiplatform_stops.stop_name_normalized;

CREATE TABLE stops_normalized AS (
	SELECT DISTINCT
		stop_code, stop_name_normalized, stop_lat_normalized, stop_lon_normalized
	FROM stops_normalized_tmp_distinct
	ORDER BY stop_code
);

---- NB!!! QUALITY CONTROL! see #14, run query in #13 and make sure you get 0 results

DROP TABLE stops_normalized_tmp_distinct;





-- CLEANING

--- TODO: any cleaning to do?





-- ENHANCING

--- add ward details to stops
INSTALL spatial;
LOAD spatial;

ALTER TABLE stops_normalized ADD COLUMN ward_number VARCHAR;

UPDATE stops_normalized
	SET ward_number = wards.number
	FROM (
		SELECT OBJECTID AS id, NAME AS name, WARD AS number, geom -- only actually need the number and geom columns, including others if we want
		FROM ST_Read('data/source/city-of-ottawa/wards_2022_to_2026.geojson')
	) wards
	WHERE ST_Within(ST_Point(stop_lon_normalized, stop_lat_normalized), wards.geom);




-- OUTPUT

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
			source, service_id, service_window, t.route_id, t.direction_id, t.trip_headsign, stop_times.stop_code, stops_normalized.stop_lat_normalized, stops_normalized.stop_lon_normalized, stops_normalized.ward_number,
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
		r.route_id,
		r.direction_id,
		r.trip_headsign AS most_common_headsign,
		t.total_trips
	FROM ranked_headsigns r
	JOIN trip_counts t ON 
		r.source = t.source AND
		r.route_id = t.route_id AND
		r.direction_id = t.direction_id
	WHERE r.rn = 1
);

UPDATE web_routes
	SET source = 'current'
	WHERE source = 'legacy';

UPDATE web_routes
	SET source = 'new'
	WHERE source = 'nwtb';

COPY web_routes TO 'data/out/for-web/routes.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');
