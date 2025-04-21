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
        WHERE source IS NOT NULL
    );


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
CREATE TEMPORARY TABLE correction_null_stop_codes AS
	FROM read_csv('data/corrections/null_stop_codes.csv');

UPDATE stops
    SET
        stop_code = correction_null_stop_codes.stop_code
    FROM correction_null_stop_codes
    WHERE
        stops.stop_id = correction_null_stop_codes.stop_id
        AND correction_null_stop_codes.source = 'nwtb-2025-04'
        AND stops.stop_code IS NULL;

---- QC: check it worked with: `from stops where stop_code is null;`

DROP TABLE correction_null_stop_codes;


-- TODO: correct errant stop_code

    -- TKTK...

-- join trip ID and stop code from relevant tables
ALTER TABLE stop_times ADD COLUMN service_id VARCHAR;
ALTER TABLE stop_times ADD COLUMN stop_code VARCHAR;

UPDATE stop_times
	SET service_id = trips.service_id
	FROM trips
	WHERE stop_times.trip_id = trips.trip_id;

UPDATE stop_times
	SET stop_code = stops.stop_code
	FROM stops
	WHERE stop_times.stop_id = stops.stop_id;