-- assumes you have the combined database loaded

-- all stop_codes that appear only once in the dataset:
SELECT * FROM stops WHERE stop_code IN (SELECT stop_code from stops GROUP BY stop_code HAVING COUNT(*) = 1);
--	(to look for ones unique to the NWTB portion, add `AND source = 'nwtb'`)

-- all stop_names that appear for only GTFS or NWTB (with thanks to ChatGPT!):
SELECT *
· FROM stops
· WHERE stop_name IN (
·     SELECT stop_name
·     FROM stops
·     GROUP BY stop_name
·     HAVING COUNT(DISTINCT source) = 1
‣ );