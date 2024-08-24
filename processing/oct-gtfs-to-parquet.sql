-- CREATE TABLE feed_info as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-24-GTFSExport/feed_info.txt", dateformat='%Y%m%d', types={'feed_start_date': 'DATE', 'feed_end_date': 'DATE'});
-- CREATE TABLE calendar as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-24-GTFSExport/calendar.txt", dateformat='%Y%m%d', types={'start_date': 'DATE', 'end_date': 'DATE'});
-- CREATE TABLE calendar_dates as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-24-GTFSExport/calendar_dates.txt", dateformat='%Y%m%d', types={'date': 'DATE'});

CREATE TABLE stops as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-24-GTFSExport/stops.txt");
CREATE TABLE stop_times as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-24-GTFSExport/stop_times.txt", types={'trip_id': 'VARCHAR', 'stop_id': 'VARCHAR'});
CREATE TABLE trips as SELECT * FROM read_csv("data/source/octranspo-gtfs/2024-08-24-GTFSExport/trips.txt");

-- add OCT GTFS as source
ALTER TABLE stops ADD COLUMN source VARCHAR DEFAULT 'octranspo-gtfs';
ALTER TABLE stops ALTER COLUMN source DROP DEFAULT;

ALTER TABLE stop_times ADD COLUMN source VARCHAR DEFAULT 'octranspo-gtfs';
ALTER TABLE stop_times ALTER COLUMN source DROP DEFAULT;

ALTER TABLE trips ADD COLUMN source VARCHAR DEFAULT 'octranspo-gtfs';
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
CREATE TEMP TABLE gtfs_representative_services AS
    SELECT *
    FROM read_csv("data/source/octranspo-gtfs/2024-08-24-gtfs_representative_services.csv");

UPDATE gtfs_representative_services
	SET day_of_week = 'weekday'
	WHERE day_of_week = 'friday';

DELETE FROM trips
WHERE NOT (
    source = 'nwtb'
    OR
    service_id IN (SELECT service_id FROM gtfs_representative_service_ids)
);

DELETE FROM stop_times
WHERE NOT (
    source = 'nwtb'
    OR
    service_id IN (SELECT service_id FROM gtfs_representative_service_ids)
);

UPDATE trips
	SET service_id = gtfs_representative_services.day_of_week
	FROM gtfs_representative_services
	WHERE trips.service_id = gtfs_representative_services.service_id;

UPDATE stop_times
	SET service_id = gtfs_representative_services.day_of_week
	FROM gtfs_representative_services
	WHERE stop_times.service_id = gtfs_representative_services.service_id;



-- TODO:
	-- sort out blank stop codes
	-- filter source=octranspo-gtfs to just three representative services (weekday, Saturday, Sunday) [will have to figure out service_id logic; maybe re-use JS in a standalone notebook to generate a string you can paste in]
	-- dedupe stops / create a standard set of IDs and codes, to draw from GTFS lat/lng
	-- condense multi-platform/entry stops into a single one (multiple stop_id per stop_code likely best indicator) [based on which has the most stop_times?]
	-- ? pre-compute stop_times per stop (grouped by source, service, service_window) [we can also do this in-browser, but may be faster to just have a pre-computed lookup table]
	-- drop columns that aren't used in analysis before exporting

EXPORT DATABASE 'oc_transpo_gtfs' (FORMAT 'parquet', COMPRESSION 'GZIP');
