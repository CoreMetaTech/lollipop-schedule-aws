import io
import json
import urllib3
import uuid
import copy
from datetime import datetime
from db import DynamoDBHandler
from db import DynamoDBOperationError

table_name_suffix = "-2iph2dahajadpnro5xkxcbveoq-staging"
extra_event_table = "ExtraEvent" + table_name_suffix
icalendar_table = "ICalendar" + table_name_suffix
icalendar_event_table = "ICalendarEvent" + table_name_suffix
ical_freqency_table = "ICalFreqency" + table_name_suffix


def lambda_handler(event, context):
    try:
        employee_id = "fd3a73c5-9a80-4355-90f3-7daa44bdc5a5"
        is_forced_update = True

        # check employee_id in ExtraEvent table to fetch an `event` item
        # the `item` should only have 1 record in the table.
        extraEvent = _fetchExtraEvent(employee_id)
        if extraEvent == None:
            return responseJson(400, message="extra_event_not_found")

        url = extraEvent["downloadableURL"]
        lastUpdatedAt = extraEvent["lastUpdatedAt"]
        icalendarId = extraEvent["iCalendarID"]
        is_more_24_hrs = _isMoreThan24Hrs(lastUpdatedAt)

        if url:
            if icalendarId.strip() and not is_forced_update and not is_more_24_hrs:
                print("load from table directly")
                schedule = _getScheduleFromTables(icalendarId)
                return responseJson(200, schedule=schedule)
            else:
                schedule = _download_calendar(url)
                responseSchedule = copy.deepcopy(schedule)
                
                ical = schedule["calendar"]
                events = schedule["events"]
                if events:
                    savedEventIDs = _saveICalenderEvents(events)
                    ical["iCalendarEventIDs"] = savedEventIDs
                    savedId = _saveICalendar(ical, extraEvent["id"])
                    if savedId:
                        _updateExtraEvent(extraEvent["id"], savedId)

                return responseJson(200, schedule=responseSchedule)
        else:
            return responseJson(400, message="extra_event_url_not_found")
    except ValueError as err:
        return responseJson(400, message=err.args[0])

    # # Check if parameters are sent in the request
    # if 'queryStringParameters' in event and event['queryStringParameters'] is not None:
    #     # Extract parameters from the request
    #     parameters = event['queryStringParameters']

    #     employee_id = None
    #     if 'employee_id' in parameters:
    #         employee_id = parameters['employee_id']

    #         try:
    #             # check employee_id in ExtraEvent table to fetch an `event` item
    #             # the `item` should only have 1 record in the table.
    #             extraEvent = fetchExtraEvent(employee_id)
    #             url = extraEvent['downloadableURL']
    #             lastUpdatedAt = extraEvent['lastUpdatedAt']
    #             icalendarId = extraEvent['iCalendarID']
    #             is_more_10_mins = isMoreThanTenMins(lastUpdatedAt)

    #             if url:
    #                 is_forced_update = False
    #                 if 'is_forced_update' in parameters:
    #                     is_forced_update = parameters['is_forced_update']

    #                 if icalendarId and not is_forced_update and not is_more_10_mins:
    #                     print("load from table directly")
    #                     schedule = getScheduleFromTables(icalendarId)
    #                     return responseJson(200, schedule=schedule)
    #                 else:
    #                     schedule = download_calendar(url)
    #                     ical = schedule['calendar']
    #                     events = schedule['events']

    #                     if events:
    #                         savedEventIDs = saveICalenderEvents(events)
    #                         ical['iCalendarEventIDs'] = savedEventIDs
    #                         savedId = saveICalendar(ical)
    #                         updateExtraEvent(extraEvent['id'], savedId)

    #                     return responseJson(200, schedule=schedule)
    #             else:
    #                 return responseJson(400, message='extra_event_url_not_found')
    #         except ValueError as err:
    #             return responseJson(400, message=err.args[0])
    #     else:
    #         return responseJson(400, message="request_parameters_employee_id_missed")
    # else:
    #     return responseJson(400, message="No_request_parameters")


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


def _getScheduleFromTables(calendarId: str):
    ical = _get_item(icalendar_table, calendarId)

    schedule = {"type": "ICS", "calendar": None, "events": []}

    if ical:
        version = ical["version"]
        name = ical["name"]
        timeZone = ical["timeZone"]
        productID = ical["productID"]
        calendar = {
            "version": version,
            "name": name,
            "timeZone": timeZone,
            "productID": productID,
        }
        schedule["calendar"] = calendar
        schedule["events"] = []

        iCalendarEventIDs = ical["iCalendarEventIDs"]
        if iCalendarEventIDs:
            events = []
            for eventID in iCalendarEventIDs:
                event = _get_item(icalendar_event_table, eventID)
                if event:
                    freqencyID = event["freqencyID"]
                    if freqencyID:
                        freqency = _get_item(ical_freqency_table, freqencyID)
                        event["rule"] = freqency
                    events.append(event)
            schedule["events"] = events
    return schedule


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


def _saveRule(rule, uid):
    handler = DynamoDBHandler(ical_freqency_table)

    byDay = rule["byday"] if "byday" in rule.keys() else None
    exceptDates = rule["except_dates"] if "except_dates" in rule.keys() else None
    until = rule["until"] if "until" in rule.keys() else None

    item = {
        "type": rule["freq"],
        "interval": rule["interval"],
        "byDays": byDay,
        "exceptDates": exceptDates,
        "uid": uid,
        "until": until,
    }

    try:
        id = _checkExistingID(handler, "uid", uid)
        return handler.save_item(item, id)
    except DynamoDBOperationError as e:
        print("DynamoDB Save Error on saveRule:", e)
        return None


def _saveICalenderEvents(events):
    handler = DynamoDBHandler(icalendar_event_table)

    savedIDs = []
    for event in events:
        uid = event["uid"]
        startAt = event["startAt"]
        endAt = event["endAt"]
        summary = event["summary"]

        savedRuleID = None
        if "rule" in event.keys():
            rule = event["rule"]
            savedRuleID = _saveRule(rule, uid)

        item = {
            "uid": uid,
            "startAt": startAt,
            "endAt": endAt,
            "summary": summary,
            "freqencyID": savedRuleID,
        }

        try:
            id = _checkExistingID(handler, "uid", uid)
            saved_id = handler.save_item(item, id)
            savedIDs.append(saved_id)
        except DynamoDBOperationError as e:
            print("DynamoDB Save Error on _saveICalenderEvents:", e)
    return savedIDs


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
    resp = http.request("GET", url, preload_content=False)
    resp.auto_close = False
    schedule = _handle_ics(resp)
    resp.close()

    # Return the calendar data
    return schedule


def _handle_ics(resp):
    schedule = {"type": "ICS", "calendar": None, "events": []}

    # properties who will be used in following `for` loop
    previous_key = None
    event = {}
    calendar = {}
    start_load_calendar = False
    start_load_event = False

    for line in io.TextIOWrapper(resp):
        parts = line.split(":")

        if len(parts) == 1:
            if previous_key == "SUMMARY":
                line = line.strip()
                if len(line) > 0:
                    event["summary"] += line  #' '.join(line.split())
            elif previous_key == "EXDATE":
                # format EXDATE on next line if the value be splited to multiple lines.
                # e.g. "EXDATE;TZID=Europe/Paris:20240519T000000,20240414T000000,20240402T000000,2
                #   0240521T000000,20240407T000000"
                line = line.strip()
                temp_value = event["rule"]["except_dates"]
                if len(line) > 0:
                    temp_value += line

                dates = temp_value.split(",")
                formatted_strs = []
                for date_str in dates:
                    formatted_str = date_str
                    formatted_strs.append(formatted_str)
                event["rule"]["except_dates"] = formatted_strs

            previous_key = None
        elif len(parts) == 2:
            key = parts[0]
            value = " ".join(parts[1].split())

            # format EXDATE on next line if the value only one line.
            # "EXDATE;TZID=Europe/Paris:20240519T000000,20240414T000000,20240402T000000
            if previous_key == "EXDATE":
                temp_value = event["rule"]["except_dates"]
                event["rule"]["except_dates"] = temp_value.split(",")
                previous_key = None

            if key == "BEGIN" and value == "VCALENDAR":
                start_load_calendar = True
                start_load_event = False
                calendar = {
                    "version": None,
                    "name": None,
                    "timeZone": None,
                    "productID": None,
                }
            elif key == "END" and value == "VCALENDAR":
                schedule["calendar"] = calendar
                break
            elif key == "BEGIN" and value == "VEVENT":
                start_load_calendar = False
                start_load_event = True
                event = {
                    "uid": None,
                    "startAt": None,
                    "endAt": None,
                    "is_cancelled": False,
                    "summary": None,
                    "timeZone": calendar["timeZone"],
                }
            elif key == "END" and value == "VEVENT":
                if not event["is_cancelled"] and event["uid"]:
                    schedule["events"].append(event)

            # parse `calendar`
            if start_load_calendar:
                if key == "NAME":
                    calendar["name"] = value
                elif key == "TIMEZONE-ID":
                    calendar["timeZone"] = value
                elif key == "VERSION":
                    calendar["version"] = value
                elif key == "PRODID":
                    calendar["productID"] = value

            # parse `event`
            if start_load_event:
                if key == "UID":
                    event["uid"] = value
                if key == "DTSTART;TZID={0}".format(calendar["timeZone"]):
                    event["startAt"] = value
                if key == "DTEND;TZID={0}".format(calendar["timeZone"]):
                    event["endAt"] = value
                if key == "STATUS":
                    event["is_cancelled"] = value == "CANCELLED"
                if key == "SUMMARY":
                    event["summary"] = value
                    previous_key = "SUMMARY"
                # RRULE:FREQ=WEEKLY;INTERVAL=1;BYDAY=TU,SU
                if key == "RRULE":
                    rule = {}
                    sub_parts = value.split(";")
                    for item in sub_parts:
                        # FREQ=WEEKLY
                        params = item.split("=")
                        key2 = params[0].lower()
                        value2 = params[1].split(",")
                        if len(value2) == 1:
                            rule[key2] = " ".join(value2)
                        else:
                            rule[key2] = value2
                    event["rule"] = rule
                    previous_key = "RRULE"
                # EXDATE;TZID=Europe/Paris:20240414T000000,20240402T000000,20240407T000000
                if (
                    key == "EXDATE;TZID={0}".format(calendar["timeZone"])
                    and previous_key == "RRULE"
                ):
                    # save as temporary value, will handle it on next loop with next line's value.
                    # because the next may still EXDATE (splited to multiple lines)
                    event["rule"]["except_dates"] = value
                    previous_key = "EXDATE"
    return schedule
