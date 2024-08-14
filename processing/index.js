import fs from 'fs'
import { parseFromString } from 'dom-parser'

const fileJson = JSON.parse(fs.readFileSync('data/schedules/weekday/1-Direction1.json'))

const dom = parseFromString(fileJson.Html)

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

console.log(extractTimetableFromDom(dom))
console.log(extractTripIdsFromDom(dom).length)
