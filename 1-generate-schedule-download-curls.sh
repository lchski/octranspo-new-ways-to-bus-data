for schedule in $(cat data/routes/weekday.json | jq -c '.[] | {Date: "2025-9-15", RouteDirection: {Key: .RouteDirection.Key}, TimingPointsOnly: false}')
do
	route_direction=$(echo $schedule | jq -r '.RouteDirection.Key.Identifier + "-" + .RouteDirection.Key.Direction')
	printf "curl 'https://plan.octranspo.com/plan/RouteTimetable/RequestRouteTimetable' -H 'content-type: application/json' --data-raw '%s' > data/schedules/weekday/%s.json\n\nsleep 2\n" $schedule $route_direction
done

for schedule in $(cat data/routes/saturday.json | jq -c '.[] | {Date: "2025-9-13", RouteDirection: {Key: .RouteDirection.Key}, TimingPointsOnly: false}')
do
	route_direction=$(echo $schedule | jq -r '.RouteDirection.Key.Identifier + "-" + .RouteDirection.Key.Direction')
	printf "curl 'https://plan.octranspo.com/plan/RouteTimetable/RequestRouteTimetable' -H 'content-type: application/json' --data-raw '%s' > data/schedules/saturday/%s.json\n\nsleep 2\n" $schedule $route_direction
done

for schedule in $(cat data/routes/sunday.json | jq -c '.[] | {Date: "2025-9-14", RouteDirection: {Key: .RouteDirection.Key}, TimingPointsOnly: false}')
do
	route_direction=$(echo $schedule | jq -r '.RouteDirection.Key.Identifier + "-" + .RouteDirection.Key.Direction')
	printf "curl 'https://plan.octranspo.com/plan/RouteTimetable/RequestRouteTimetable' -H 'content-type: application/json' --data-raw '%s' > data/schedules/sunday/%s.json\n\nsleep 2\n" $schedule $route_direction
done
