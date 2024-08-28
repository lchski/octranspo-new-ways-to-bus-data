import fs from 'fs'
import { parseFromString } from 'dom-parser'
import { stringify } from 'csv-stringify/sync'

let routes = []

fs.readdirSync('data/routes/', {
		withFileTypes: true,
		recursive: true
	})
	.filter(item => ! item.isDirectory())
	.filter(item => item.name.includes('.json'))
	.map(item => ({
		routes: JSON.parse(fs.readFileSync(item.path + '/' + item.name))
	}))
	.forEach(routeGroup => routeGroup.routes.forEach(route => routes.push(route)))

let routeDetails = routes.map(route => ({
	route_id: route.RouteDirection.Key.Identifier + '-' + route.RouteDirection.Key.Direction,
	direction_id: Number(route.RouteDirection.Key.Direction.replace('Direction', '')) - 1,
	trip_headsign: route.RouteDirection.DirectionName
}))

// via: https://stackoverflow.com/a/76772679
const dedupe_object_array = (source) => {
	if (!Array.isArray(source)) {
	   return [];
	}
	return [...new Set(source.map(o => {
	  const sortedObjectKeys = Object.keys(o).sort();
	  const obj = Object.assign({}, ...sortedObjectKeys.map(k => ({[k]: o[k]})));
	  return JSON.stringify(obj);
	}))]
	.map(s => JSON.parse(s));
  }

routeDetails = dedupe_object_array(routeDetails)

fs.writeFileSync('data/out/gtfs-routes-for-sql.csv', stringify(routeDetails, { header: true }))
