import io
import json
import urllib3
import itertools
import icalendar
from icalendar.prop import vText, vDDDLists, vDDDTypes


def _firstValueInList(list: list):
    if list and len(list) == 1:
        return list[0]
    return None


def _icalValueToString(value):
    if value is None:
        return None
    if isinstance(value, vText):
        return value.to_ical().decode("utf-8")
    if isinstance(value, vDDDTypes):
        return value.to_ical().decode("utf-8")
    elif isinstance(value, vDDDLists):
        return [d for d in value.to_ical().decode("utf-8").split(",")]
    else:
        return value.to_ical().decode("utf-8")


def _handle_ics(data):
    schedule = {"type": "ICS", "calendar": None, "events": []}

    calendar = icalendar.Calendar.from_ical(data)
    if not calendar:
        return schedule

    # parse VCALENDAR
    vcals = calendar.walk("VCALENDAR")
    if vcals:
        cal = vcals[-1]
        schedule_calendar = {
            "version": _icalValueToString(cal.get("VERSION")),
            "name": _icalValueToString(cal.get("NAME")),
            "timeZone": _icalValueToString(cal.get("TIMEZONE-ID")),
            "productID": _icalValueToString(cal.get("PRODID")),
        }

        schedule["calendar"] = schedule_calendar

    # parse VEVENT
    for event in calendar.walk("VEVENT"):
        # STATUS:CANCELLED
        isCancelled = event.get("STATUS") and "CANCELLED" in event.get("STATUS")

        if not isCancelled:
            schedule_event = {
                "uid": _icalValueToString(event.get("UID")),
                "startAt": _icalValueToString(event.get("DTSTART")),
                "endAt": _icalValueToString(event.get("DTEND")),
                "summary": _icalValueToString(event.get("SUMMARY")),
                "rule": None,
            }

            exdates = _icalValueToString(event.get("EXDATE"))

            rule = event.get("RRULE")
            if rule:
                schedule_event["rule"] = {
                    "freq": _firstValueInList(rule.get("FREQ")),
                    "interval": _firstValueInList(rule.get("INTERVAL")),
                    "byday": rule.get("BYDAY"),
                    "until": _firstValueInList(rule.get("UNTIL")),
                    "count": _firstValueInList(rule.get("COUNT")),
                    "except_dates": exdates,
                }
            schedule["events"].append(schedule_event)

    return schedule


if __name__ == "__main__":
    http = urllib3.PoolManager()

    url = "https://hupf8gmctj.execute-api.eu-west-1.amazonaws.com/prod/getCalendarIcalEvents?calendarId=-NkoH4OGWbdZSE1CdkJp&businessId=-NTEziw1tmha-pY4uE15&key=BWXFBLs3f4ezQjbNcape5SIzkdL8mAUL"
    http = urllib3.PoolManager()
    resp = http.request("GET", url)
    schedule = _handle_ics(resp.data)

    # f = open("ical_ex.ics", "r")
    # data = f.read()
    # f.close()
    # schedule = _handle_ics(data)

    # print(schedule)
    print(json.dumps(schedule))

    # resp = http.request("GET", url)
    # schedule = {"type": "ICS", "calendar": None, "events": []}
    # calendar = icalendar.Calendar.from_ical(resp.data)
    # calcal = calendar.walk("VCALENDAR")
    # for cal in calcal:
    #     print(cal.get("NAME"))
    #     print(cal.get("TIMEZONE-ID"))
    #     print(cal.get("VERSION"))
    #     print(cal.get("PRODID"))
    # print("----")

    # for event in calendar.walk("VEVENT"):
    #     isCancelled = event.get("STATUS") and "CANCELLED" in event.get("STATUS")
    #     print(isCancelled)

    #     if isCancelled:
    #         continue

    #     print(event.get("SUMMARY"))
    #     print(event.decoded("DTSTART"))
    #     print(event.decoded("DTEND"))

    #     rule = event.get("RRULE")
    #     if rule:
    #         print(_firstValueInList(rule.get("FREQ")))
    #         print(_firstValueInList(rule.get("INTERVAL")))
    #         print(_firstValueInList(rule.get("COUNT")))
    #         print(rule.get("BYDAY"))
    #     print("----")

    # print(json.dumps(schedule))
