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
