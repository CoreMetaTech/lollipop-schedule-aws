import io
import json
import urllib3
import uuid
import copy
import itertools
from datetime import datetime
from datetime import timedelta
from db import DynamoDBHandler
from db import DynamoDBOperationError
import icalendar
from icalendar.prop import vText, vDDDLists, vDDDTypes

from dateutil.rrule import rrulestr
from datetime import datetime, timedelta
from dateutil.parser import parse as date_parse

import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeSerializer

table_name_suffix = "-2iph2dahajadpnro5xkxcbveoq-staging"
extra_event_table = "ExtraEvent" + table_name_suffix
icalendar_table = "ICalendar" + table_name_suffix
icalendar_event_table = "ICalendarEvent" + table_name_suffix
ical_freqency_table = "ICalFreqency" + table_name_suffix
ical_event_status_table = "ICalendarEventStatus" + table_name_suffix


def lambda_handler(event, context):
    if "queryStringParameters" in event and event["queryStringParameters"] is not None:
        # Extract parameters from the request
        parameters = event["queryStringParameters"]

        employee_id = None
        if "employee_id" in parameters:
            employee_id = parameters["employee_id"]

            extraEvent = _fetchExtraEvent(employee_id)
            if extraEvent == None:
                return responseJson(400, message="extra_event_not_found")

            try:
                url = extraEvent["downloadableURL"]

                if url:
                    schedule = _download_calendar(url)
                    schedule["load_from_db"] = False

                    events = schedule["events"]
                    for item in events:
                        status = _getEventStatus(item["uid"], item["startAt"])
                        item["eventStatus"] = (
                            status if status is not None else "SCHEDULED"
                        )
                    return responseJson(200, schedule=schedule)
                else:
                    return responseJson(400, message="extra_event_url_not_found")
            except ValueError as err:
                return responseJson(400, message=err.args[0])
        else:
            return responseJson(400, message="request_parameters_employee_id_missed")
    else:
        return responseJson(400, message="No_request_parameters")


# def lambda_handler(event, context):
#     if "queryStringParameters" in event and event["queryStringParameters"] is not None:
#         # Extract parameters from the request
#         parameters = event["queryStringParameters"]

#         employee_id = None
#         if "employee_id" in parameters:
#             employee_id = parameters["employee_id"]

#             extraEvent = _fetchExtraEvent(employee_id)
#             if extraEvent == None:
#                 return responseJson(400, message="extra_event_not_found")

#             url = extraEvent["downloadableURL"]
#             lastUpdatedAt = extraEvent["lastUpdatedAt"]
#             icalendarId = extraEvent.get("iCalendarID")
#             is_more_24_hrs = _isMoreThan24Hrs(lastUpdatedAt)

#             date_from = parameters.get("start", None)
#             date_to = parameters.get("end", None)
#             date_from, date_to = format_dates(date_from, date_to)

#             try:
#                 if url:
#                     is_forced_update = False
#                     if "is_forced_update" in parameters:
#                         is_forced_update = parameters["is_forced_update"]
#                         is_forced_update = is_forced_update.lower() == "true"

#                     if (
#                         icalendarId
#                         and icalendarId.strip()
#                         and not is_forced_update
#                         and not is_more_24_hrs
#                     ):
#                         schedule = _getScheduleFromTables(
#                             icalendarId, date_from, date_to
#                         )
#                         schedule["load_from_db"] = True
#                         return responseJson(200, schedule=schedule)
#                     else:
#                         schedule = _download_calendar(url)
#                         schedule["load_from_db"] = False

#                         # save iCalendar
#                         ical = schedule["calendar"]
#                         savedICalId = _saveICalendar(ical, extraEvent["id"])

#                         # save iCalendarEvents
#                         if savedICalId:
#                             # update ExtraEvent with new iCalendar ID
#                             _updateExtraEvent(extraEvent["id"], savedICalId)

#                             # save all Events
#                             events = schedule["events"]
#                             if events:
#                                 saved_ids = _saveICalenderEvents(events, savedICalId)

#                             # format respnse schedule
#                             del schedule["calendar"]["id"]
#                             del schedule["calendar"]["createdAt"]
#                             del schedule["calendar"]["updatedAt"]
#                             del schedule["calendar"]["extraEventID"]
#                             for item in events:
#                                 if "is_cancelled" in item.keys():
#                                     del item["is_cancelled"]
#                                 status = _getEventStatus(item["uid"])
#                                 item["eventStatus"] = (
#                                     status if status is not None else "SCHEDULED"
#                                 )

#                         return responseJson(200, schedule=schedule)
#                 else:
#                     return responseJson(400, message="extra_event_url_not_found")
#             except ValueError as err:
#                 return responseJson(400, message=err.args[0])
#         else:
#             return responseJson(400, message="request_parameters_employee_id_missed")
#     else:
#         return responseJson(400, message="No_request_parameters")


def format_dates(date_from, date_to):
    if date_from and not date_to:
        d_f = datetime.strptime(date_from, "%Y%m%dT%H%M%S")
        date_to = (d_f + timedelta(days=8)).date().strftime("%Y%m%dT%H%M%S")
    elif not date_from and date_to:
        d_t = datetime.strptime(date_to, "%Y%m%dT%H%M%S")
        date_from = (d_t - timedelta(days=7)).date().strftime("%Y%m%dT%H%M%S")
    elif not date_from and not date_to:
        date_from = datetime.now().strftime("%Y%m%dT%H%M%S")
        date_to = (datetime.now() + timedelta(days=8)).date().strftime("%Y%m%dT%H%M%S")
    return date_from, date_to


def responseJson(statusCode, schedule=None, message=None):
    return {
        "statusCode": statusCode,
        "body": json.dumps({"schedule": schedule, "message": message}),
    }


def _updateExtraEvent(id: str, savedICalID: str):
    handler = DynamoDBHandler(extra_event_table)
    item_key = {"id": id}
    update_expressions = {
        "iCalendarID": savedICalID,
        "lastUpdatedAt": handler.generate_AWSDateTime(),
    }

    try:
        updated_attributes = handler.update_item(
            key=item_key,
            update_expressions=update_expressions,
        )
    except DynamoDBOperationError as e:
        print("updateExtraEvent - updateExtraEventDynamoDB Operation Error:", e)


def _checkExistingID(handler: DynamoDBHandler, key: str, value):
    existing = handler.search_items({key: value})
    existing = existing[0] if existing else None
    if existing and "id" in existing.keys():
        return existing["id"]
    return None


def _get_item(table: str, id: str) -> dict:
    handler = DynamoDBHandler(table)
    try:
        return handler.get_item({"id": id})
    except:
        return None


def _get_iCalenderEvents(iCalendarID: str):
    handler = DynamoDBHandler(icalendar_event_table)
    return handler.search_items({"iCalendarID": iCalendarID})


def _get_iCal_rule(uid: str):
    handler = DynamoDBHandler(ical_freqency_table)
    rule = handler.search_items({"uid": uid})
    return rule[0] if rule and len(rule) > 0 else None


def _getScheduleFromTables(calendarId: str, start: str = None, end: str = None):
    ical = _get_item(icalendar_table, calendarId)

    schedule = {"type": "ICS", "calendar": None, "events": []}

    if ical:
        version = ical["version"]
        name = ical["name"]
        timeZone = ical["timeZone"]
        productID = ical["productID"]
        calendar = {
            "version": str(version) if version is not None else "",
            "name": name,
            "timeZone": timeZone,
            "productID": productID,
        }
        schedule["calendar"] = calendar
        schedule["events"] = []

        # load events
        events = []
        saved_events = []
        if start and end:
            saved_events = _search_items_betweens(start, end, calendarId)
        else:
            saved_events = _get_iCalenderEvents(calendarId)

        for event in saved_events:
            uid = event["uid"]
            freqency = _get_iCal_rule(uid)
            if freqency:
                event["rule"] = {
                    "freq": freqency["type"],
                    "interval": (
                        int(freqency.get("interval"))
                        if freqency.get("interval")
                        else None
                    ),
                    "count": (
                        int(freqency.get("count")) if freqency.get("count") else None
                    ),
                    "byday": freqency["byDays"],
                    "except_dates": freqency["exceptDates"],
                    "until": freqency["until"],
                }

            status = _getEventStatus(uid)
            events.append(
                {
                    "uid": event["uid"],
                    "eventStatus": (status if status is not None else "SCHEDULED"),
                    "startAt": event["startAt"],
                    "endAt": event["endAt"],
                    "summary": event["summary"],
                    "rule": event.get("rule"),
                    "timeZone": timeZone,
                }
            )
        schedule["events"] = events
    return schedule


def _getEventStatus(uid: str, startDateStr: str):
    handler = DynamoDBHandler(ical_event_status_table)
    try:
        items = handler.search_items({"uid": uid, "startAt": startDateStr})
        if items:
            return items[0]["status"]
        return None
    except DynamoDBOperationError as e:
        print("Error searching items == " + e)
        return None


def _isMoreThan24Hrs(lastUpdatedAt) -> bool:
    now = datetime.now()
    # 2024-05-08T23:13:00.000Z
    if lastUpdatedAt[-1] == "Z" and lastUpdatedAt[-5] == ".":
        date = datetime.strptime(lastUpdatedAt[:-5], "%Y-%m-%dT%H:%M:%S")
    else:
        raise ValueError("Invalid lastUpdatedAt date format")
    duration = now - date
    duration_in_s = duration.total_seconds()
    days = divmod(duration_in_s, 86400)[0]
    return days >= 1.0


def _durationSecondsFromNow(timeStr, format: str) -> bool:
    now = datetime.now()
    try:
        date = datetime.strptime(timeStr, format)
        duration = date - now
        return duration.total_seconds()
    except Exception as e:
        print("error on _durationSecondsFromNow ::: ", e)
        return None


def _saveRule(rule, uid):
    handler = DynamoDBHandler(ical_freqency_table)

    byDay = rule["byday"] if "byday" in rule.keys() else None
    exceptDates = rule["except_dates"] if "except_dates" in rule.keys() else None
    until = rule["until"] if "until" in rule.keys() else None
    count = rule["count"] if "count" in rule.keys() else None

    if byDay and isinstance(byDay, str):
        byDay = [byDay]

    item = {
        "type": rule["freq"],
        "interval": rule["interval"],
        "byDays": byDay,
        "exceptDates": exceptDates,
        "uid": uid,
        "until": until,
        "count": count,
    }

    try:
        id = _checkExistingID(handler, "uid", uid)
        return handler.save_item(item, id)
    except DynamoDBOperationError as e:
        print("DynamoDB Save Error on saveRule:", e)
        return None


def _saveICalenderEvents(events, iCalendarID):
    handler = DynamoDBHandler(icalendar_event_table)

    saved_ids = []
    for event in events:
        uid = event["uid"]
        startAt = event["startAt"]
        endAt = event["endAt"]
        summary = event["summary"]

        # convert startAt & endAt to AWS datetime
        s = datetime.strptime(startAt, "%Y%m%dT%H%M%S")
        e = datetime.strptime(endAt, "%Y%m%dT%H%M%S")
        startDateTime = handler.generate_AWSDateTime(s)
        endDateTime = handler.generate_AWSDateTime(e)

        item = {
            "uid": uid,
            "startAt": startAt,
            "endAt": endAt,
            "summary": summary,
            "iCalendarID": iCalendarID,
            "startDateTime": startDateTime,
            "endDateTime": endDateTime,
        }

        try:
            id = _checkExistingID(handler, "uid", uid)
            saved_id = handler.save_item(item, id)

            rule = event.get("rule")
            if rule:
                rule = event["rule"]
                _saveRule(rule, uid)

            saved_ids.append(saved_id)
        except DynamoDBOperationError as e:
            print("DynamoDB Save Error on _saveICalenderEvents:", e)
    return saved_ids


def _saveICalendar(ical, extraEventID):
    handler = DynamoDBHandler(icalendar_table)
    try:
        id = _checkExistingID(handler, "extraEventID", extraEventID)
        ical["extraEventID"] = extraEventID
        return handler.save_item(ical, id)
    except DynamoDBOperationError as e:
        print("saveICalendar DynamoDB Save Error:", e)
        return None


def _fetchExtraEvent(employee_id: str):
    handler = DynamoDBHandler(extra_event_table)

    try:
        items = handler.search_items({"employeeID": employee_id})
        if items:
            if len(items) > 1:
                raise ValueError("More than one employee found in ExtraEvent table")
            item = items[0]
            schedule_type = item["extraDataType"]
            if schedule_type == "ICS":
                return item
            else:
                print("Unsupported schedule type")
                return None
    except DynamoDBOperationError as e:
        print("_fetchExtraEvent DynamoDB Operation Error:", e)
        return None


def _download_calendar(url: str):
    # Download the calendar from the URL

    http = urllib3.PoolManager()
    resp = http.request("GET", url)
    schedule = _handle_ics(resp)

    # Return the calendar data
    return schedule


# def _handle_ics(resp):
#     schedule = {"type": "ICS", "calendar": None, "events": []}

#     calendar = icalendar.Calendar.from_ical(resp.data)

#     # parse VCALENDAR
#     vcals = calendar.walk("VCALENDAR")
#     if vcals:
#         cal = vcals[-1]
#         schedule_calendar = {
#             "version": _icalValueToString(cal.get("VERSION")),
#             "name": _icalValueToString(cal.get("NAME")),
#             "timeZone": _icalValueToString(cal.get("TIMEZONE-ID")),
#             "productID": _icalValueToString(cal.get("PRODID")),
#         }

#         schedule["calendar"] = schedule_calendar

#     # parse VEVENT
#     for event in calendar.walk("VEVENT"):
#         # STATUS:CANCELLED
#         isCancelled = event.get("STATUS") and "CANCELLED" in event.get("STATUS")

#         if not isCancelled:
#             schedule_event = {
#                 "uid": _icalValueToString(event.get("UID")),
#                 "startAt": _icalValueToString(event.get("DTSTART")),
#                 "endAt": _icalValueToString(event.get("DTEND")),
#                 "summary": _icalValueToString(event.get("SUMMARY")),
#                 "rule": None,
#                 "timeZone": schedule["calendar"].get("timeZone"),
#             }

#             exdates = _icalValueToString(event.get("EXDATE"))

#             rule = event.get("RRULE")
#             if rule:
#                 # check if the rule has expired
#                 until = _firstValueInList(rule.get("UNTIL"))
#                 if until:
#                     until_from_now = _durationSecondsFromNow(until, "%Y%m%dT%H%M%SZ")
#                     if until_from_now == None or until_from_now < 0:
#                         continue

#                 schedule_event["rule"] = {
#                     "freq": _firstValueInList(rule.get("FREQ")),
#                     "interval": _firstValueInList(rule.get("INTERVAL")),
#                     "byday": rule.get("BYDAY"),
#                     "until": _firstValueInList(rule.get("UNTIL")),
#                     "count": _firstValueInList(rule.get("COUNT")),
#                     "except_dates": exdates,
#                 }
#             schedule["events"].append(schedule_event)

#     return schedule


# def _icalValueToString(value):
#     if value is None:
#         return None
#     if isinstance(value, vText):
#         return value.to_ical().decode("utf-8")
#     if isinstance(value, vDDDTypes):
#         return value.to_ical().decode("utf-8")
#     elif isinstance(value, vDDDLists):
#         return [d for d in value.to_ical().decode("utf-8").split(",")]
#     else:
#         return value.to_ical().decode("utf-8")


def list_occurrences_next_one_months(
    rrule_string, start_date, latest_date=None, exdate_str=None
):
    """
    List all occurrences of a recurring event within the next two months,
    respecting the UNTIL clause and excluding any dates specified in EXDATE.

    :param rrule_string: The RRULE string that defines the recurrence.
    :param start_date: The start date of the event.
    :param exdate_str: The EXDATE string containing dates to exclude (optional).
    :return: A list of datetime objects representing each occurrence within the next two months.
    """
    # Parse the recurrence rule and start date
    rule = rrulestr(rrule_string, dtstart=start_date)

    # Parse EXDATEs if provided
    exdates = []
    if exdate_str:
        exdates = [date_parse(date_str) for date_str in exdate_str.split(",")]

    # Determine the current date and the date two months from now
    now = datetime.now()
    end_date = latest_date if latest_date else now + timedelta(days=30)

    # Determine the end date based on UNTIL or two months from now
    until_date = rule._until if rule._until else end_date

    # Generate occurrences within the range and filter out EXDATEs
    occurrences = [
        dt
        for dt in rule.between(now, min(end_date, until_date), inc=True)
        if dt not in exdates
    ]

    return occurrences


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


def _handle_ics(resp):
    schedule = {"type": "ICS", "calendar": None, "events": []}

    calendar = icalendar.Calendar.from_ical(resp.data)

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
    rule_events = []
    latest_date = datetime.now()
    timezone = schedule["calendar"].get("timeZone")
    for cal_event in calendar.walk("VEVENT"):
        schedule_event = handle_calendar_event(cal_event, timezone)

        if schedule_event is None:
            continue
        elif schedule_event == "RULE":
            rule_events.append(cal_event)
        else:
            endAt = schedule_event["endAt"]
            latest_date = latest_event_date(endAt, latest_date)
            schedule["events"].append(schedule_event)

    schedule_rule_events = []
    for event in rule_events:
        events = handle_rule_event(event, latest_date, timezone)
        if events:
            schedule_rule_events = schedule_rule_events + events

    schedule["events"] = schedule["events"] + schedule_rule_events
    return schedule


def latest_event_date(endAt, latest_date):
    # get latest date - using for repeat event without until_date
    try:
        endDate = (
            datetime.strptime(endAt, "%Y%m%dT%H%M%S") if endAt is not None else None
        )
        latest_date = max(endDate, latest_date) if endDate is not None else latest_date
        return latest_date
    except Exception as e:
        print("error on convert latest_date :::: ", e)
        return latest_date


def handle_calendar_event(event, timezone):
    try:
        # STATUS:CANCELLED
        isCancelled = event.get("STATUS") and "CANCELLED" in event.get("STATUS")

        if not isCancelled:
            # save event if has rule for later parse
            rule = event.get("RRULE")
            if rule is None:
                return {
                    "uid": _icalValueToString(event.get("UID")),
                    "startAt": _icalValueToString(event.get("DTSTART")),
                    "endAt": _icalValueToString(event.get("DTEND")),
                    "summary": _icalValueToString(event.get("SUMMARY")),
                    "timeZone": timezone,
                }
            else:
                return "RULE"
    except Exception as e:
        print("error in handle_calendar_event ::: ", e)
        return None


def handle_rule_event(event, latest_date, timezone):
    try:
        rule = _icalValueToString(event.get("RRULE"))
        startAt = _icalValueToString(event.get("DTSTART"))
        endAt = _icalValueToString(event.get("DTEND"))
        exdates = _icalValueToString(event.get("EXDATE"))
        exdates = ",".join(exdates)

        start = datetime.strptime(startAt, "%Y%m%dT%H%M%S")
        end = datetime.strptime(endAt, "%Y%m%dT%H%M%S")
        gap = end - start

        ruleDates = list_occurrences_next_one_months(rule, start, latest_date, exdates)

        result = []
        for ruleDate in ruleDates:
            schedule_event = {
                "uid": _icalValueToString(event.get("UID")),
                "startAt": ruleDate.strftime("%Y%m%dT%H%M%S"),
                "endAt": (ruleDate + gap).strftime("%Y%m%dT%H%M%S"),
                "summary": _icalValueToString(event.get("SUMMARY")),
                "timeZone": timezone,
            }
            result.append(schedule_event)
        return result
    except Exception as e:
        print("error in handle_rule_event ::: ", e)
        return None


def _firstValueInList(list: list):
    if list and len(list) == 1:
        return list[0]
    return None


def _search_items_betweens(start_date: str, end_date: str, calendarID: str):
    """

    Args:
        start_date (str): date str formatted like `20240523T100000`
        end_date (str): date str formatted like `20240523T100000`
    """

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(icalendar_event_table)

    input_format = "%Y%m%dT%H%M%S"
    db_format = "%Y-%m-%dT%H:%M:%S.000Z"

    # Define the date range
    start_datetime = datetime.strptime(start_date, input_format)
    end_datetime = datetime.strptime(end_date, input_format)

    start_date_str = start_datetime.strftime(db_format)
    end_date_str = end_datetime.strftime(db_format)

    try:
        response = table.scan(
            FilterExpression=Attr("startDateTime").lte(end_date_str)
            & Attr("endDateTime").gte(start_date_str)
            & Attr("iCalendarID").eq(calendarID)
        )
        items = response.get("Items", [])
        return items
    except ClientError as e:
        raise DynamoDBOperationError(f"Error searching items: {e}")
    except Exception as e:
        raise DynamoDBOperationError(f"An unexpected error occurred: {e}")


def convert_int(s):
    try:
        return int(s)
    except ValueError:
        return None
