curl 'https://plan.octranspo.com/plan/api/RouteTimetableAPI/GetRouteDirections?date=2025-9-15' | jq > data/routes/weekday.json

curl 'https://plan.octranspo.com/plan/api/RouteTimetableAPI/GetRouteDirections?date=2025-9-13' | jq > data/routes/saturday.json

curl 'https://plan.octranspo.com/plan/api/RouteTimetableAPI/GetRouteDirections?date=2025-9-14' | jq > data/routes/sunday.json
