import json
from typing import Dict, Any, Optional, List
from datetime import datetime
from db import DynamoDBHandler, DynamoDBOperationError
from notification_sender.send_notification import OneSignalNotificationSender
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_NAME_SUFFIX = "-2iph2dahajadpnro5xkxcbveoq-staging"
ICAL_EVENT_STATUS_TABLE = f"ICalendarEventStatus{TABLE_NAME_SUFFIX}"
DEVICE_INFO_TABLE = f"DeviceInfo{TABLE_NAME_SUFFIX}"
USER_TABLE = f"User{TABLE_NAME_SUFFIX}"
EMPLOYEE_TABLE = f"Employee{TABLE_NAME_SUFFIX}"
PRELOAD_STATUS = ["COMPLETE", "CANCEL", "DELAY", "NOTSHOW", "SCHEDULED", "RESCHEDULE"]


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    try:
        if event["httpMethod"] == "GET":
            return handle_get_request(event)
        elif event["httpMethod"] == "POST":
            return handle_post_request(event)
        else:
            return response_json(405, message="method_not_allowed")
    except ValueError as e:
        logger.error(f"Invalid input: {str(e)}")
        return response_json(400, message=str(e))
    except DynamoDBOperationError as e:
        logger.error(f"Database error: {str(e)}")
        return response_json(500, message="database_error")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return response_json(500, message="internal_server_error")


def handle_get_request(event: Dict[str, Any]) -> Dict[str, Any]:
    query_params = event.get("queryStringParameters", {})
    event_uid = query_params.get("uid")
    start_at = query_params.get("startAt")

    if not event_uid or not start_at:
        return response_json(400, message="request_parameters_missed - uid or startAt")

    status = _get_event_status(event_uid, start_at)
    return response_json(
        200, {"uid": event_uid, "startAt": start_at, "eventStatus": status}
    )


def handle_post_request(event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return response_json(400, message="request_body_invalid_json")

    uid = body["uid"]
    start_at = body["startAt"]
    new_status = body["status"].upper()
    test_external_id = body.get("external_id")

    if not uid or not start_at or not new_status:
        return response_json(400, message="missing_required_fields")

    if new_status not in PRELOAD_STATUS:
        return response_json(400, message=f"unsupported_status: {new_status}")

    try:
        _update_status(uid, start_at, new_status)
        owner_emails = _get_owner_emails()

        if test_external_id and test_external_id not in owner_emails:
            owner_emails.append(test_external_id)

        if owner_emails:
            _send_push_notification(owner_emails, start_at, new_status)
        return response_json(
            200, {"uid": uid, "startAt": start_at, "eventStatus": new_status}
        )
    except DynamoDBOperationError as e:
        logger.error(f"DynamoDB operation error: {str(e)}")
        return response_json(500, message="database_error")


def response_json(
    statusCode: int,
    response: Optional[Dict[str, Any]] = None,
    message: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "statusCode": statusCode,
        "body": json.dumps({"response": response, "message": message}),
    }


def _get_event_status(eventUID: str, startAt: str) -> Optional[str]:
    handler = DynamoDBHandler(ICAL_EVENT_STATUS_TABLE)
    try:
        items = handler.search_items({"uid": eventUID, "startAt": startAt})
        if items:
            return items[0]["status"]
        logger.info(f"No status found for event UID: {eventUID}, startAt: {startAt}")
        return None
    except DynamoDBOperationError as e:
        logger.error(f"Error searching items: {e}")
        return None


def _update_status(
    eventUID: str, startAt: str, status: str
) -> Optional[Dict[str, Any]]:
    handler = DynamoDBHandler(ICAL_EVENT_STATUS_TABLE)

    try:
        key = {"uid": eventUID, "startAt": startAt}
        id = handler.get_item_id(key)

        item = {"uid": eventUID, "startAt": startAt, "status": status}
        result = handler.save_item(item, id)
        logger.info(
            f"Status updated for event UID: {eventUID}, startAt: {startAt}, new status: {status}"
        )
        return result
    except DynamoDBOperationError as e:
        logger.error(f"Error updating status: {e}")
        raise


def _send_push_notification(
    owner_emails: List[str], event_start_at: str, status: str
) -> bool:
    event_start_at = format_date_string(event_start_at)
    message = f"Event started at: {event_start_at} has updated to: {status}"
    title = "Event status updated."

    try:
        sender = OneSignalNotificationSender()
        response = sender.send_notification_by_external_ids(
            owner_emails, message, message
        )
        logger.info(
            f"Push notification sent for event starting at {event_start_at}, new status: {status}, with response: {response}"
        )
        return True
    except Exception as e:
        logger.error(f"Error sending push notification: {e}")
        return False


def format_date_string(date_string: str) -> str:
    dt = datetime.strptime(date_string, "%Y%m%dT%H%M%S")
    return dt.strftime("%H:%M %d %b, %Y")


def _get_owner_emails() -> List[str]:
    try:
        employee_handler = DynamoDBHandler(EMPLOYEE_TABLE)
        user_handler = DynamoDBHandler(USER_TABLE)
        device_info_handler = DynamoDBHandler(DEVICE_INFO_TABLE)

        owners = employee_handler.search_items({"role": "OWNER"}) or []
        managers = employee_handler.search_items({"role": "MANAGER"}) or []
        owners_and_managers = owners + managers

        users = []
        user_ids = [
            employee["userID"]
            for employee in owners_and_managers
            if employee.get("userID")
        ]
        for user_id in user_ids:
            user = user_handler.get_item({"id": user_id})
            users.append(user)

        emails = []
        for user in users:
            if user.get("email"):
                devices = device_info_handler.search_items({"userID": user["id"]})
                if any(device.get("isOn", False) for device in devices):
                    emails.append(user["email"])

        return emails
    except DynamoDBOperationError as e:
        logger.error(f"Error searching items: {e}")
        return []
