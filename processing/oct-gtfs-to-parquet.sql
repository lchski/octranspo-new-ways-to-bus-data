-- CREATE TABLE feed_info as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-26-GTFSExport/feed_info.txt", dateformat='%Y%m%d', types={'feed_start_date': 'DATE', 'feed_end_date': 'DATE'});
-- CREATE TABLE calendar as SELECT * FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/calendar.txt", dateformat='%Y%m%d', types={'start_date': 'DATE', 'end_date': 'DATE'});
-- CREATE TABLE calendar_dates as SELECT * FROM read_csv("data/source/octranspo-legacy-gtfs/2024-08-26-google_transit/calendar_dates.txt", dateformat='%Y%m%d', types={'date': 'DATE'});

CREATE TABLE stops as
	SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon
	FROM read_csv("data/source/octranspo-legacy-gtfs/2024-09-29-google_transit/stops.txt");
CREATE TABLE stop_times as
	SELECT trip_id, arrival_time, stop_id, stop_sequence
	FROM read_csv("data/source/octranspo-legacy-gtfs/2024-09-29-google_transit/stop_times.txt", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});
CREATE TABLE trips as
	SELECT route_id, service_id, trip_id, trip_headsign, direction_id -- trip_headsign probably not useful? but maybe if we're trying to join / infer directions later
	FROM read_csv("data/source/octranspo-legacy-gtfs/2024-09-29-google_transit/trips.txt");

-- add OCT GTFS as source
ALTER TABLE stops ADD COLUMN source VARCHAR DEFAULT 'octranspo-legacy-gtfs';
ALTER TABLE stops ALTER COLUMN source DROP DEFAULT;

ALTER TABLE stop_times ADD COLUMN source VARCHAR DEFAULT 'octranspo-legacy-gtfs';
ALTER TABLE stop_times ALTER COLUMN source DROP DEFAULT;

ALTER TABLE trips ADD COLUMN source VARCHAR DEFAULT 'octranspo-legacy-gtfs';
ALTER TABLE trips ALTER COLUMN source DROP DEFAULT;

-- load the NWTB data
INSERT INTO
	stops (stop_code, stop_id, stop_name)
	SELECT * FROM read_csv("data/out/gtfs-stops.csv");

INSERT INTO
	stop_times (trip_id, arrival_time, stop_id, stop_sequence)
	SELECT * EXCLUDE (stop_code) FROM read_csv("data/out/gtfs-stop-times.csv");

INSERT INTO
	trips (route_id, service_id, trip_id)
	SELECT * FROM read_csv("data/out/gtfs-trips.csv");

-- add NWTB as source
UPDATE stops
	SET source = 'nwtb'
	WHERE source IS NULL;

UPDATE stop_times
	SET source = 'nwtb'
	WHERE source IS NULL;

UPDATE trips
	SET source = 'nwtb'
	WHERE source IS NULL;

-- normalize NWTB data to GTFS format
UPDATE stop_times
	SET arrival_time = arrival_time[12:19]
	WHERE source = 'nwtb';



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


-- join in the better route detail data
CREATE TEMPORARY TABLE temp_routes AS
	FROM read_csv('data/out/gtfs-routes-for-sql.csv');

UPDATE trips
SET
	trip_headsign = temp_routes.trip_headsign,
	direction_id = temp_routes.direction_id
FROM temp_routes
WHERE
	trips.route_id = temp_routes.route_id
	AND trips.source = 'nwtb';

DROP TABLE temp_routes;

-- join in the corrected stop_code for stops with null stop_code
CREATE TEMPORARY TABLE correction_null_stop_codes AS
	FROM read_csv('data/corrections/null_stop_codes.csv');

UPDATE stops
SET
	stop_code = correction_null_stop_codes.stop_code
FROM correction_null_stop_codes
WHERE
	stops.stop_id = correction_null_stop_codes.stop_id
	AND stops.source = correction_null_stop_codes.source
	AND stops.stop_code IS NULL;

---- QC: check it worked with: `from stops where stop_code is null;`

DROP TABLE correction_null_stop_codes;

-- join in the corrected stop_code for stops with incorrect stop_code
CREATE TEMPORARY TABLE correction_errant_stop_codes AS
	FROM read_csv('data/corrections/errant_stop_codes.csv', all_varchar = true);

UPDATE stops
SET
	stop_code = correction_errant_stop_codes.stop_code_corrected
FROM correction_errant_stop_codes
WHERE
	stops.stop_id = correction_errant_stop_codes.stop_id
	AND stops.source = correction_errant_stop_codes.source
	AND stops.stop_code = correction_errant_stop_codes.stop_code_current;

DROP TABLE correction_errant_stop_codes;

-- standardize route IDs to enable comparison
UPDATE trips
	SET route_id = REGEXP_REPLACE(route_id, '-350$', '')
	WHERE source = 'octranspo-legacy-gtfs';

UPDATE trips
	SET route_id = REGEXP_REPLACE(route_id, '-Direction[12]$', '')
	WHERE source = 'nwtb';

-- fix occasional odd trip_headsign formatting
UPDATE trips
	set trip_headsign = TRIM(trip_headsign);


-- join trip ID and stop code from relevant tables
ALTER TABLE stop_times ADD COLUMN service_id VARCHAR;
ALTER TABLE stop_times ADD COLUMN stop_code VARCHAR;

UPDATE stop_times
	SET service_id = trips.service_id
	FROM trips
	WHERE stop_times.source = trips.source AND stop_times.trip_id = trips.trip_id;

UPDATE stop_times
	SET stop_code = stops.stop_code
	FROM stops
	WHERE stop_times.source = stops.source AND stop_times.stop_id = stops.stop_id;



-- filter down to just representative data
--- JSON generated with: https://observablehq.com/d/fb22d192264eb8f6
ALTER TABLE trips ADD COLUMN service_id_original VARCHAR;
UPDATE trips
	SET service_id_original = service_id;

CREATE TEMP TABLE gtfs_representative_services AS
    SELECT *
    FROM read_csv("data/source/octranspo-legacy-gtfs/2024-09-29-gtfs_representative_services.csv");

UPDATE gtfs_representative_services
	SET day_of_week = 'weekday'
	WHERE day_of_week = 'friday';

--- backup entries before deletion from main tables
---- if you want to see the "original", run: `FROM trips UNION FROM trips_unused;` or the same for stop_times
CREATE TABLE trips_unused AS
	FROM trips
	WHERE NOT (
		source = 'nwtb'
		OR
		service_id IN (SELECT service_id FROM gtfs_representative_services)
	);

CREATE TABLE stop_times_unused AS
	FROM stop_times
	WHERE NOT (
		source = 'nwtb'
		OR
		service_id IN (SELECT service_id FROM gtfs_representative_services)
	);

--- remove the backed-up entries
DELETE FROM trips
WHERE NOT (
    source = 'nwtb'
    OR
    service_id IN (SELECT service_id FROM gtfs_representative_services)
);

DELETE FROM stop_times
WHERE NOT (
    source = 'nwtb'
    OR
    service_id IN (SELECT service_id FROM gtfs_representative_services)
);

UPDATE trips
	SET service_id = gtfs_representative_services.day_of_week
	FROM gtfs_representative_services
	WHERE trips.service_id = gtfs_representative_services.service_id;

UPDATE stop_times
	SET service_id = gtfs_representative_services.day_of_week
	FROM gtfs_representative_services
	WHERE stop_times.service_id = gtfs_representative_services.service_id;




-- NORMALIZING

--- normalize stops to draw from stop_code, not stop_id
CREATE TEMP TABLE stop_ids_normalized AS (
  SELECT source, stop_code, stop_id as stop_id_normalized FROM (
      WITH stop_counts AS (
      select source, stop_id, count(*) as n_stops from stop_times group by all
    )
      SELECT
        s.source,
        s.stop_code,
        s.stop_id,
        sc.n_stops,
        ROW_NUMBER() OVER (PARTITION BY s.source, s.stop_code ORDER BY sc.n_stops DESC) AS n_stops_rank
      FROM stops s
      JOIN stop_counts sc ON s.source = sc.source AND s.stop_id = sc.stop_id
  )
    WHERE n_stops_rank = 1
  );

CREATE TEMP TABLE stops_normalized_tmp AS (
	SELECT
		s.source,
		s.stop_code,
		s.stop_id,
		s.stop_name,
		s.stop_lat,
		s.stop_lon,
		s_ids.stop_id_normalized
	FROM stops s
	JOIN stop_ids_normalized s_ids ON
		s.source = s_ids.source AND
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
		sn.source = s.source AND
		sn.stop_id_normalized = s.stop_id;

CREATE TEMP TABLE stops_normalized_tmp_distinct AS (
	SELECT DISTINCT
		source,
		stop_code,
		stop_id_normalized,
		stop_name_normalized,
		stop_lat_normalized,
		stop_lon_normalized
	FROM stops_normalized_tmp
);

DROP TABLE stop_ids_normalized;
DROP TABLE stops_normalized_tmp;

--- pull stop_lat and stop_lon from OCT-legacy entries for the NWTB entries
UPDATE stops_normalized_tmp_distinct sn
	SET
		stop_lat_normalized = sn_oct.stop_lat_normalized,
		stop_lon_normalized = sn_oct.stop_lon_normalized
	FROM (
		SELECT * FROM stops_normalized_tmp_distinct
		WHERE source = 'octranspo-legacy-gtfs'
	) sn_oct
	WHERE
		sn.source = 'nwtb' AND
		sn.stop_lat_normalized IS NULL AND
		sn.stop_lon_normalized IS NULL AND
		sn.stop_code = sn_oct.stop_code;

--- pull from the full stops file for any missing stop details (NB: uses stop_code AND stop_id to ensure no duplicates)
UPDATE stops_normalized_tmp_distinct sn
	SET
		stop_lat_normalized = stops_entire.stop_lat,
		stop_lon_normalized = stops_entire.stop_lon
	FROM read_csv('data/out/gtfs-stops-entire.csv') stops_entire
	WHERE
		sn.source = 'nwtb' AND
		sn.stop_lat_normalized IS NULL AND
		sn.stop_lon_normalized IS NULL AND
		sn.stop_id_normalized = stops_entire.stop_id AND
		sn.stop_code = stops_entire.stop_code;

--- a final pass using the full stops file, only for errant stop codes (when test ran, this affected one stop, which we'd given a stop code of `ERR1`)
UPDATE stops_normalized_tmp_distinct sn
	SET
		stop_lat_normalized = stops_entire.stop_lat,
		stop_lon_normalized = stops_entire.stop_lon
	FROM read_csv('data/out/gtfs-stops-entire.csv') stops_entire
	WHERE
		sn.source = 'nwtb' AND
		sn.stop_lat_normalized IS NULL AND
		sn.stop_lon_normalized IS NULL AND
		sn.stop_id_normalized = stops_entire.stop_id AND
		contains(sn.stop_code, 'ERR');

--- fix stop names for multiplatform stops
UPDATE stops_normalized_tmp_distinct sn
	SET
		stop_name_normalized = correction_multiplatform_stops.stop_name_corrected
	FROM read_csv('data/corrections/multiplatform_stop_names.csv', all_varchar = true) correction_multiplatform_stops
	WHERE
		sn.source = correction_multiplatform_stops.source AND
		sn.stop_code = correction_multiplatform_stops.stop_code AND
		sn.stop_id_normalized = correction_multiplatform_stops.stop_id_normalized AND
		sn.stop_name_normalized = correction_multiplatform_stops.stop_name_normalized;

--- fix one errant stop name that messed up deduplication (see #13), manually (lol)
UPDATE stops_normalized_tmp_distinct sn
	SET
		stop_name_normalized = 'BANK / GLEBE'
	WHERE
		source = 'nwtb' AND
		stop_code = '6843' AND
		stop_id_normalized = 'CF090' AND
		stop_name_normalized = 'GLEBE / BANK';

CREATE TABLE stops_normalized AS (
	SELECT DISTINCT
		stop_code, stop_name_normalized, stop_lat_normalized, stop_lon_normalized
	FROM stops_normalized_tmp_distinct
	ORDER BY stop_code
);

---- NB!!! QUALITY CONTROL! see #14, run query in #13 and make sure you get 0 results

DROP TABLE stops_normalized_tmp_distinct;

-- CLEANING
--- remove stop / stop times associated with a set of "auto-generated" trips
DELETE FROM stop_times
WHERE (
	source = 'octranspo-gtfs'
	AND
	stop_id IN ('1', '9489')
);

DELETE FROM stops
WHERE (
	source = 'octranspo-gtfs'
	AND
	stop_id IN ('1', '9489')
);

DELETE FROM trips
WHERE (
	source = 'octranspo-gtfs'
	AND
	route_id = '109'
	AND
	trip_headsign = 'Auto Generated-01'
)

-- TODO:
	-- likely just for viz? filter out R1?

EXPORT DATABASE 'data/out/oc_transpo_gtfs' (FORMAT 'parquet', COMPRESSION 'GZIP');



COPY stops_normalized TO 'data/out/for-web/stops_normalized.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');

CREATE TEMP TABLE web_stop_times_by_stop AS (
	SELECT
		source, service_id, service_window, stop_code, count(*) as n_stop_times
	FROM (SELECT DISTINCT(*) FROM stop_times)
	GROUP BY ALL
	ORDER BY source, service_id, service_window, stop_code
);

UPDATE web_stop_times_by_stop
	SET source = 'current'
	WHERE source = 'octranspo-legacy-gtfs';

UPDATE web_stop_times_by_stop
	SET source = 'new'
	WHERE source = 'nwtb';

COPY web_stop_times_by_stop TO 'data/out/for-web/stop_times_by_stop.parquet' (FORMAT 'parquet', COMPRESSION 'GZIP');

DROP TABLE web_stop_times_by_stop;
