import fs from 'fs'
import { parseFromString } from 'dom-parser'
import { stringify } from 'csv-stringify/sync'

const start = Date.now()

console.log("processing began at", start)

const parseDomFromFile = (path) => {
	const fileJson = JSON.parse(fs.readFileSync(path))

	return parseFromString(fileJson.Html)
}

const extractTimetableFromDom = (dom) => JSON.parse(
	dom.getElementById('RouteTimetableContainer')
		.getAttribute('data-route-timetable-view-model')
		.replaceAll('&quot;', '"')
)

const extractTripIdsFromDom = (dom) => {
	const scriptTags = dom.getElementsByTagName('script')

	const hastinfoTag = scriptTags[1] // TODO: build a check in for this?

	const tripIdsRegex = /hastinfo.routeTimetable.tripsKeys = (\[(?:"[0-9_]*?",?)*\])/;
	
	return JSON.parse(hastinfoTag.innerHTML.match(tripIdsRegex)[1])
}

const schedules = fs.readdirSync('data/schedules/', {
		withFileTypes: true,
		recursive: true
	})
	.filter(item => ! item.isDirectory())
	.filter(item => item.name.includes('.json'))
	.map(item => ({
		schedule_id: item.name.replace('.json', ''),
		service: item.path.split('/')[2],
		path: item.path + '/' + item.name
	}))

const stopCodeRegex = / - \(([0-9]{0,4})\)$/ // need the 0 in `{0,4}` to account for stops that just have a stop code of ` - ()`

const processStopsFromTimetable = (timetable) => timetable.StopViewModels.map((stop, i) => {
	// console.log("stop.DisplayText", stop.DisplayText)

	return {
		stop_index: i,
		name: stop.DisplayText.replace(stopCodeRegex, ''), // TODO / NB: this may cause a bug if " - " is found in a stop name when not separating the stop label and code
		code: stop.DisplayText.match(stopCodeRegex)[1],
		id: stop.Identifier,
		displayText: stop.DisplayText
	}
})

let testSchedules = schedules.slice(0,250)
const scheduleData = testSchedules.map(schedule => {
// const scheduleData = schedules.map(schedule => {
	const scheduleDom = parseDomFromFile(schedule.path)
	
	const timetable = extractTimetableFromDom(scheduleDom)
	const stops = processStopsFromTimetable(timetable)
	// console.log('processing', schedule.path)

	const scheduleDataWithoutTrips = {
		...schedule,
		stops: stops,
		timesAtStops: Object.values(timetable.PassingTimeViewModelsByStops).map((timesAtStop, i) => ({
			stopIndex: i,
			stopCode: stops[i].code,
			times: timesAtStop.map(timeAtStop => timeAtStop.Time) // NB: this isn't stop_times in the GTFS sense, these are "all the times a bus stops at stop `i`"
		})),
		timetableTripCount: timetable.TripsCount,
		tripIds: extractTripIdsFromDom(scheduleDom)
	}

	return {
		...schedule,
		timetableTripCount: scheduleDataWithoutTrips.timetableTripCount,
		stops: scheduleDataWithoutTrips.stops,
		trips: scheduleDataWithoutTrips.tripIds.map((tripId, tripIndex) => ({
			id: tripId,
			stopTimes: scheduleDataWithoutTrips.timesAtStops.map((timesAtStop, stopIndex) => ({
				stopCode: timesAtStop.stopCode,
				arrivalTime: timesAtStop.times[tripIndex],
				stopSequence: stopIndex
			}))
		}))
	}
})

fs.writeFileSync('data/out/schedules.json', JSON.stringify(scheduleData))

let gtfsStops = []
let gtfsTrips = []
let gtfsStopTimes = []

scheduleData.forEach((schedule => {
	gtfsStops.push(...schedule.stops.map(stop => ({
		stop_id: stop.id,
		stop_code: stop.code,
		stop_name: stop.name,
		display_name: stop.displayText
	})))

	gtfsTrips.push(...schedule.trips.map(trip => ({
		route_id: schedule.schedule_id,
		service_id: schedule.service,
		trip_id: trip.id
	})))

	schedule.trips.forEach(trip => gtfsStopTimes.push(...trip.stopTimes.map(stopTime => ({
		trip_id: trip.id,
		arrival_time: stopTime.arrivalTime,
		stop_code: stopTime.stopCode,
		stop_sequence: stopTime.stopSequence
	}))))
}))

// dedupe the stops
gtfsStops = [...new Set(gtfsStops)]

// filter out n/a stops and incorporate the stop_id
gtfsStopTimes = gtfsStopTimes
	.filter(stop_time => stop_time.arrival_time !== '0001-01-01T00:00:00Z')
	.map(stop_time => ({
		...stop_time,
		stop_id: gtfsStops.find(stop => stop.stop_code === stop_time.stop_code).stop_id
	}))

fs.writeFileSync('data/out/gtfs-stops.json', JSON.stringify(gtfsStops))
fs.writeFileSync('data/out/gtfs-trips.json', JSON.stringify(gtfsTrips))
fs.writeFileSync('data/out/gtfs-stop-times.json', JSON.stringify(gtfsStopTimes))

fs.writeFileSync('data/out/gtfs-stops.csv', stringify(gtfsStops, { header: true }))
fs.writeFileSync('data/out/gtfs-trips.csv', stringify(gtfsTrips, { header: true }))
fs.writeFileSync('data/out/gtfs-stop-times.csv', stringify(gtfsStopTimes, { header: true }))

const end = Date.now()

console.log("processing began and ended at", [start, end])
console.log("processing took", [(end - start) / 1000], "seconds")
