-- assumes you have the combined database loaded

-- all stop_codes that appear only once in the dataset:
SELECT * FROM stops WHERE stop_code IN (SELECT stop_code from stops GROUP BY stop_code HAVING COUNT(*) = 1);
--	(to look for ones unique to the NWTB portion, add `AND source = 'nwtb'`)

-- all stop_names that appear for only GTFS or NWTB (with thanks to ChatGPT!):
SELECT *
	FROM stops
	WHERE stop_name IN (
		SELECT stop_name
		FROM stops
		GROUP BY stop_name
		HAVING COUNT(DISTINCT source) = 1
	);

SELECT stop_code, COUNT(stop_code)
	FROM stops
	WHERE stop_name IN (
		SELECT stop_name
		FROM stops
		GROUP BY stop_name
		HAVING COUNT(DISTINCT source) = 1
	);


-- looking for similar stop names, to figure out if we can reconcile them?
WITH stop_pairs AS (
  SELECT 
    a.stop_name AS name1,
    a.stop_code AS code1,
    a.stop_id AS id1,
    b.stop_name AS name2,
    b.stop_code AS code2,
    b.stop_id AS id2,
    levenshtein(LOWER(a.stop_name), LOWER(b.stop_name)) AS distance
  FROM 
    stops a
    JOIN stops b ON LEFT(a.stop_name, 1) = LEFT(b.stop_name, 1)
  WHERE 
    a.stop_name < b.stop_name  -- Avoid self-comparisons and duplicates
    AND ABS(LENGTH(a.stop_name) - LENGTH(b.stop_name)) <= 3  -- Only compare strings with similar lengths
),
similarity_scores AS (
  SELECT 
    name1,
    code1,
    id1,
    name2,
    code2,
    id2,
    distance,
    1 - (distance::FLOAT / GREATEST(LENGTH(name1), LENGTH(name2))) AS similarity
  FROM 
    stop_pairs
  WHERE
    distance <= 5  -- Only calculate similarity for strings with small Levenshtein distance
)
SELECT 
  name1,
  code1,
  id1,
  name2,
  code2,
  id2,
  distance,
  ROUND(similarity::NUMERIC, 2) AS similarity
FROM 
  similarity_scores
WHERE 
  similarity >= 0.8  -- Adjust this threshold as needed
ORDER BY 
  similarity DESC, distance
LIMIT 
  100;  -- Adjust the limit to see more or fewer results

-- adjusted version that compares between groups
--- NB: this has the stop_name self-comparison check commented out, see explanation below
WITH stop_pairs AS (
  SELECT 
    a.stop_name AS name1,
    a.stop_code AS code1,
    a.stop_id AS id1,
    a.source AS source1,
    b.stop_name AS name2,
    b.stop_code AS code2,
    b.stop_id AS id2,
    b.source AS source2,
    levenshtein(LOWER(a.stop_name), LOWER(b.stop_name)) AS distance
  FROM 
    stops a
    JOIN stops b ON LEFT(a.stop_name, 1) = LEFT(b.stop_name, 1)
  WHERE 
    a.source < b.source  -- Ensure we only compare across different sources
    -- AND a.stop_name < b.stop_name  -- Avoid self-comparisons and duplicates (NB: this may be undesirable, because the goal is partly to find ones that likely _are_ the same, but have different codes / IDs)
    AND ABS(LENGTH(a.stop_name) - LENGTH(b.stop_name)) <= 3  -- Only compare strings with similar lengths
),
similarity_scores AS (
  SELECT 
    name1,
    code1,
    id1,
    source1,
    name2,
    code2,
    id2,
    source2,
    distance,
    1 - (distance::FLOAT / GREATEST(LENGTH(name1), LENGTH(name2))) AS similarity
  FROM 
    stop_pairs
  WHERE
    distance <= 5  -- Only calculate similarity for strings with small Levenshtein distance
)
SELECT 
  name1,
  code1,
  id1,
  source1,
  name2,
  code2,
  id2,
  source2,
  distance,
  ROUND(similarity::NUMERIC, 2) AS similarity
FROM 
  similarity_scores
WHERE 
  similarity >= 0.85  -- Adjust this threshold as needed
ORDER BY 
  similarity DESC, distance
LIMIT 
  100;  -- Adjust the limit to see more or fewer results

  -- possible final WHERE clauses for above, depending on goal:
    -- find stop codes that _haven't_ changed (IDs may be different): similarity = 1 AND code1 = code2
    -- find stop codes that have definitely changed OR share the same name but are legitimately different stops (e.g., diff side of same street): similarity = 1 AND code1 != code2

-- same as above, into a temp table
CREATE TEMP TABLE similar_stops AS
  WITH stop_pairs AS (
    SELECT 
      a.stop_name AS name1,
      a.stop_code AS code1,
      a.stop_id AS id1,
      a.source AS source1,
      b.stop_name AS name2,
      b.stop_code AS code2,
      b.stop_id AS id2,
      b.source AS source2,
      levenshtein(LOWER(a.stop_name), LOWER(b.stop_name)) AS distance
    FROM 
      stops a
      JOIN stops b ON LEFT(a.stop_name, 1) = LEFT(b.stop_name, 1)
    WHERE 
      a.source < b.source  -- Ensure we only compare across different sources
      -- AND a.stop_name < b.stop_name  -- Avoid self-comparisons and duplicates (NB: this may be undesirable, because the goal is partly to find ones that likely _are_ the same, but have different codes / IDs)
      AND ABS(LENGTH(a.stop_name) - LENGTH(b.stop_name)) <= 3  -- Only compare strings with similar lengths
  ),
  similarity_scores AS (
    SELECT 
      name1,
      code1,
      id1,
      source1,
      name2,
      code2,
      id2,
      source2,
      distance,
      1 - (distance::FLOAT / GREATEST(LENGTH(name1), LENGTH(name2))) AS similarity
    FROM 
      stop_pairs
    WHERE
      distance <= 5  -- Only calculate similarity for strings with small Levenshtein distance
  )
  SELECT 
    name1,
    code1,
    id1,
    source1,
    name2,
    code2,
    id2,
    source2,
    distance,
    ROUND(similarity::NUMERIC, 2) AS similarity
  FROM 
    similarity_scores
  WHERE 
    similarity >= 0.85  -- Adjust this threshold as needed
  ORDER BY 
    similarity DESC, distance;

--- comparing new to old stops, which don't share a stop ID? i.e., which need addressing
FROM (PIVOT stops ON source GROUP BY stop_id) where nwtb != "octranspo-gtfs";
FROM (PIVOT stops ON source GROUP BY stop_id) where nwtb != "octranspo-legacy-gtfs";




-- figuring out route_id and direction_id
copy (
  select
    source, route_id, direction_id, count(*)
  from trips
  group by source, route_id, direction_id
  order by source, route_id, direction_id
) to 'data/out/tmp.csv';
  -- for GTFS, see that there are ~4 entries per route, half suffixed by `-1`
  -- to compare, can use the unchanged routes listed on the site / CBC: https://www.cbc.ca/news/canada/ottawa/oc-transpo-route-change-cut-2024-otrain-trillium-1.7155003

-- trying to figure out service changes / data quality issues
select source, service_id, count(*) from trips group by source, service_id ORDER BY service_id, source DESC;
select source, service_id, count(*) from stop_times group by source, service_id ORDER BY service_id, source DESC;

--- !! a clue: looking at trips for route 305 for 2024-09-13 in the route planner 
--- https://plan.octranspo.com/plan/RouteSchedules?Date=2024-9-13&RouteKey=305~~Direction2&ShowOptions=false&TimingPointsOnly=false
--- works for both directions (CARLINGWOOD etc or NORTH GOWER etc)
--- there are three trips each direction in the database `from trips where route_id LIKE '305%';`
--- but there's only one trip each direction in the trip planner!
--- but _why_...
--- (decent indicator is the multiple trips with the exact same arrival_time—though that seemed to be a somewhat normal case for school routes, IIRC)
--- ** 2024-08-26: it was a bug in my Observable date handling! bahahaha. of course. note that by doing Friday as "representative" service we'll lose a few shopper routes—could manually add back in


-- stops that don't have any stop_times
FROM stops ANTI JOIN stop_times USING (stop_id);

-- stop times with missing trip info
FROM stop_times ANTI JOIN trips USING (trip_id);

-- trips with no stop times
FROM trips ANTI JOIN stop_times USING (trip_id);

-- rough comparison of trips between the two sources (also enables figuring out Direction1/2 maybe?)
select source, route_id, direction_id, trip_headsign, count(*) from trips group by all order by route_id, source;



-- wild query to find the most frequently used stops (regardless of differing stop_id for the same stop_code) [thanks Claude!]
WITH stop_times_count AS (
    SELECT source, service_id, stop_code, COUNT(*) AS n
    FROM stop_times
    GROUP BY source, service_id, stop_code
    ORDER BY n DESC
),
stop_names_ranked AS (
    SELECT 
        st.source,
        st.stop_code,
        s.stop_name,
        COUNT(*) AS stop_count,
        ROW_NUMBER() OVER (PARTITION BY st.source, st.stop_code ORDER BY COUNT(*) DESC) AS rn
    FROM stop_times st
    JOIN stops s ON st.source = s.source AND st.stop_id = s.stop_id
    GROUP BY st.source, st.stop_code, s.stop_name
)
SELECT 
    stc.source, 
    stc.service_id, 
    stc.stop_code, 
    snr.stop_name, 
    stc.n
FROM stop_times_count stc
LEFT JOIN stop_names_ranked snr ON stc.source = snr.source 
    AND stc.stop_code = snr.stop_code
    AND snr.rn = 1
ORDER BY stc.n DESC;

-- number of stops per trip
select t.*, st.n_stops
  from trips t
  join (select trip_id, count(*) as n_stops from stop_times group by trip_id) st
  on t.trip_id = st.trip_id;

-- 
select t.*, st.n_stops, st.trip_start
  from trips t 
  join (
    select trip_id, count(*) as n_stops, min(arrival_time) as trip_start
    from (from stop_times order by trip_id, stop_sequence)
    group by trip_id
  ) st
  on t.trip_id = st.trip_id;
  --- e.g. where route_id = '2'

CREATE MACRO route_trip_details(route_id_param) AS TABLE
  select t.*, st.n_stops, st.trip_start
    from trips t
    join (
      select trip_id, count(*) as n_stops, min(arrival_time) as trip_start
      from (from stop_times order by trip_id, stop_sequence)
      group by trip_id
    ) st
    on t.trip_id = st.trip_id
    where route_id = route_id_param::VARCHAR
    order by source, service_id, trip_start;

CREATE MACRO trip_stop_details(trip_id_param) AS TABLE
  SELECT st.*, s.stop_name
  FROM stop_times st
  JOIN stops s
  ON st.source = s.source AND st.stop_id = s.stop_id
  WHERE trip_id = trip_id_param
  ORDER BY stop_sequence;

SELECT st.* FROM stop_times WHERE trip_id = trip_id_param;

SELECT st.*, s.stop_name FROM stop_times st JOIN stops s ON st.source = s.source AND st.stop_id = s.stop_id WHERE trip_id = trip_id_param order by stop_sequence;

pivot route_trip_details(502) on source using sum(n_stops) group by service_id order by service_id desc;




-- finding all the stop_codes related to a given stop_code?
CREATE MACRO stop_codes_related_to_stop_code(stop_code_param) AS TABLE
  WITH trips_at_stop AS (
      SELECT DISTINCT trip_id
      FROM stop_times
      WHERE stop_code = stop_code_param::VARCHAR
  ),
  all_stops_for_trips AS (
      SELECT DISTINCT st.stop_code
      FROM stop_times st
      JOIN trips_at_stop tas ON st.trip_id = tas.trip_id
      WHERE st.stop_code != stop_code_param::VARCHAR
  )
  SELECT DISTINCT s.stop_code, s.stop_name
  FROM all_stops_for_trips asft
  JOIN stops s ON asft.stop_code = s.stop_code
  ORDER BY s.stop_code;

-- maaaaybe another version of the above? these are just hard to figure out independently lol
CREATE MACRO stop_codes_related_to_stop_code_2(stop_code_param) AS TABLE
  WITH trip_stops AS (
    SELECT DISTINCT st.trip_id, st.stop_code, st.source
    FROM stop_times st
    WHERE st.stop_code = stop_code_param::VARCHAR
),
related_stops AS (
    SELECT 
        ts.source,
        st.stop_code,
        s.stop_name,
        COUNT(*) as visit_count
    FROM trip_stops ts
    JOIN stop_times st ON ts.trip_id = st.trip_id AND ts.source = st.source
    JOIN stops s ON st.stop_id = s.stop_id AND st.source = s.source
    WHERE st.stop_code != stop_code_param::VARCHAR
    GROUP BY ts.source, st.stop_code, s.stop_name
)
SELECT 
    source,
    stop_code,
    stop_name,
    visit_count,
    RANK() OVER (PARTITION BY source ORDER BY visit_count DESC) as rank
FROM related_stops
ORDER BY source, visit_count DESC, stop_code




WITH stop_times_count AS (
    SELECT source, stop_code, COUNT(*) AS n
    FROM stop_times
    GROUP BY source, stop_code
    ORDER BY n DESC
),
stop_names_ranked AS (
    SELECT 
        st.source,
        st.stop_code,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        COUNT(*) AS stop_count,
        ROW_NUMBER() OVER (PARTITION BY st.source, st.stop_code ORDER BY COUNT(*) DESC) AS rn
    FROM stop_times st
    JOIN stops s ON st.source = s.source AND st.stop_id = s.stop_id
    GROUP BY st.source, st.stop_code, s.stop_name, s.stop_lat, s.stop_lon
)
SELECT 
    stc.source, 
    stc.stop_code, 
    snr.stop_name, 
    snr.stop_lat,
    snr.stop_lon
FROM stop_times_count stc
LEFT JOIN stop_names_ranked snr ON stc.source = snr.source 
    AND stc.stop_code = snr.stop_code
    AND snr.rn = 1
ORDER BY stc.n DESC;


WITH stop_times_count AS (
    SELECT source, stop_code, COUNT(*) AS n
    FROM stop_times
    GROUP BY source, stop_code
    ORDER BY n DESC
),
stop_names_ranked AS (
    SELECT 
        st.source,
        st.stop_code,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        COUNT(*) AS stop_count,
        ROW_NUMBER() OVER (PARTITION BY st.source, st.stop_code ORDER BY COUNT(*) DESC) AS rn
    FROM stop_times st
    JOIN stops s ON st.source = s.source AND st.stop_id = s.stop_id
    GROUP BY st.source, st.stop_code, s.stop_name, s.stop_lat, s.stop_lon
)
SELECT 
    stc.source, 
    stc.stop_code, 
    snr.stop_name, 
    snr.stop_lat,
    snr.stop_lon
FROM stop_times_count stc
LEFT JOIN stop_names_ranked snr ON stc.source = snr.source 
    AND stc.stop_code = snr.stop_code
    AND snr.rn = 1
ORDER BY stc.n DESC;



SELECT 
        st.source,
        st.stop_code,
        s.stop_name,
        COUNT(*) AS stop_count,
        ROW_NUMBER() OVER (PARTITION BY st.source, st.stop_code ORDER BY COUNT(*) DESC) AS rn
    FROM stop_times st
    JOIN stops s ON st.source = s.source AND st.stop_id = s.stop_id
    GROUP BY st.source, st.stop_code, s.stop_name


-- stops with number of times a bus stops at each stop
-- NB: will exclude stops with no stop_times: `FROM stops ANTI JOIN stop_times USING (stop_id);`
WITH stop_counts AS (
  select source, stop_id, count(*) as n_stops from stop_times group by all
)
  SELECT
    s.stop_id,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    s.source,
    sc.n_stops,
    ROW_NUMBER() OVER (PARTITION BY s.source, s.stop_code ORDER BY sc.n_stops DESC) AS n_stops_rank
  FROM stops s
  JOIN stop_counts sc ON s.source = sc.source AND s.stop_id = sc.stop_id;

-- create a "stop_id_normalized" lookup table
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


SELECT
    s.source,
    s.stop_id,
    s_ids.stop_id_normalized,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon
  FROM stops s
  JOIN stop_ids_normalized s_ids ON
    s.source = s_ids.source AND
    s.stop_code = s_ids.stop_code
  ;



COPY (WITH platform_counts AS (select source, stop_code, count(*) as n_platforms from stops group by all)
  SELECT
    s.source,
    s.stop_id,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    pcs.n_platforms
  FROM stops s
  JOIN platform_counts pcs ON
    s.source = pcs.source AND
    s.stop_code = pcs.stop_code
  WHERE n_platforms > 1
  ORDER BY s.stop_code, s.source, s.stop_name)
  TO 'data/out/multi-platform-stops.csv';


FROM (PIVOT stops_normalized ON source USING first(stop_name_normalized) GROUP BY stop_code) WHERE nwtb != "octranspo-legacy-gtfs";


COPY (
  SELECT DISTINCT(stop_code) FROM (
    select source, stop_code, count(*) as n_platforms from stops group by all having n_platforms > 1
  )
  ORDER BY stop_code
) TO 'data/out/multi-platform-stop-codes.csv';

FROM stops_normalized_tmp_distinct WHERE stop_code IN read_csv('data/out/multi-platform-stop-codes.csv')

COPY (
  SELECT
    source, stop_code, stop_id_normalized, stop_name_normalized, '' AS stop_name_corrected
  FROM stops_normalized_tmp_distinct
  WHERE
    stop_code IN (FROM read_csv('data/out/multi-platform-stop-codes.csv'))
  ORDER BY stop_code, source
) TO 'data/corrections/raw-multiplatform_stop_names.csv';

SELECT
  source, service_id, service_window, stop_code, count(*) as n_stop_times
  FROM stop_times
  GROUP BY ALL
  ORDER BY source, service_id, service_window, stop_code;

PIVOT (
  SELECT source, stop_code, SUM(n_stop_times) as n_stop_times
      FROM web_stop_times_by_stop
      WHERE
        list_contains(['peak_afternoon'], service_window) AND
        list_contains(['weekday', 'saturday'], service_id)
      GROUP BY stop_code, source ORDER BY stop_code
  )
  ON source
  USING SUM(n_stop_times)
;

PIVOT (
  SELECT source, stop_code, n_stop_times
      FROM web_stop_times_by_stop
      WHERE
        list_contains(['peak_afternoon'], service_window) AND
        list_contains(['weekday', 'saturday'], service_id)
  )
  ON source
  USING SUM(n_stop_times)
;


SELECT * REPLACE (ifnull("current", 0) as "n_stops_current", ifnull("new", 0) as "n_stops_new") FROM (
  WITH stop_frequencies AS (
    PIVOT (
      FROM web_stop_times_by_stop
      WHERE
        list_contains(['peak_afternoon'], service_window) AND
        list_contains(['weekday', 'saturday'], service_id)
    )
    ON source
    USING SUM(n_stop_times)
    GROUP BY stop_code
  )
  FROM stops_normalized s
  LEFT JOIN stop_frequencies sf USING(stop_code)
)
;

--- same as above, rewritten by Claude to not use PIVOT because it was throwing errors in JS
WITH stop_frequencies AS (
  SELECT 
    stop_code,
    SUM(CASE WHEN source = 'current' THEN n_stop_times ELSE 0 END)::INTEGER AS current,
    SUM(CASE WHEN source = 'new' THEN n_stop_times ELSE 0 END)::INTEGER AS new
  FROM web_stop_times_by_stop
  WHERE 
    list_contains(['peak_afternoon'], service_window) AND
    list_contains(['weekday', 'saturday'], service_id)
  GROUP BY stop_code
)
SELECT 
  s.*,
  COALESCE(sf.current, 0) AS n_stops_current,
  COALESCE(sf.new, 0) AS n_stops_new
FROM stops_normalized s
LEFT JOIN stop_frequencies sf USING(stop_code);


WITH stop_frequencies AS (
  SELECT stop_code, ifnull("current", 0) as "current", ifnull("new", 0) as "new"
  FROM (
    PIVOT (
      FROM web_stop_times_by_stop
      WHERE
        list_contains(['peak_afternoon'], service_window) AND
        list_contains(['weekday', 'saturday'], service_id)
    )
    ON source
    USING SUM(n_stop_times)
    GROUP BY stop_code
  )
  )
  FROM stops_normalized s
  LEFT JOIN stop_frequencies sf USING(stop_code);

SELECT stop_code, ifnull("current", 0) as "current", ifnull("new", 0) as "new" FROM (PIVOT (
    FROM web_stop_times_by_stop
    WHERE
      list_contains(['peak_afternoon'], service_window) AND
      list_contains(['weekday', 'saturday'], service_id)
  )
  ON source
  USING SUM(n_stop_times)
  GROUP BY stop_code
)
;

