import json
import logging
import boto3
from datetime import datetime
from db import DynamoDBHandler
from db import DynamoDBOperationError
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event, context):
    error = None
    try:
        # Get user ID and email from Cognito PostAuthentication event
        auth_user_id = event["request"]["userAttributes"][
            "sub"
        ]  # Cognito user ID (sub)
        email = event["request"]["userAttributes"]["email"]  # Cognito user email

        handler = DynamoDBHandler("ReferCode-2iph2dahajadpnro5xkxcbveoq-staging")
        try:
            # search refer code by email, get the first item if it exists
            refer_codes = handler.search_items({"email": email.lower()})
            referObj = refer_codes[0] if refer_codes and len(refer_codes) > 0 else None
        except DynamoDBOperationError as e:
            logger.error("Error searching refer code: %s", e)
            referObj = None

        if referObj:
            refer_code = referObj["code"]
            if referObj.get("available") is True:
                user_id = _create_user_details(
                    auth_user_id, email, referObj.get("firstName"), refer_code
                )
                store_id = referObj.get("storeID")
                employee_id = _create_employee(store_id, user_id)
                if employee_id:
                    downloadableURL = referObj.get("downloadableURL", "")
                    extra_event_id = _create_extra_event(employee_id, downloadableURL)
                    if extra_event_id == None:
                        error = "download iCalendar failed, cannot create event."
                else:
                    error = "create employee record failed"
            else:
                error = "refer_code_expired"
        else:
            error = "No_request_parameters"

        if error:
            logger.error("Error: %s", error)

        # Always return event for Cognito triggers
        return event

    except ClientError as e:
        logger.error("Error creating user record: %s", e)
        return event


def _create_user_details(
    auth_user_id: str, user_email: str, firstName: str = None, refer_code: str = None
):
    table = "User-2iph2dahajadpnro5xkxcbveoq-staging"
    handler = DynamoDBHandler(table)

    existing_id = _checkExistingID(handler, {"authUserID": auth_user_id})
    if existing_id:
        return existing_id

    user = {
        "authUserID": auth_user_id,
        "email": user_email,
        "firstName": firstName,
        "referCode": refer_code,
    }

    try:
        return handler.save_item(user)
    except DynamoDBOperationError as e:
        print("DynamoDB Save Error on create_user_details: ")
        print(e)
        return None


def _checkExistingID(handler: DynamoDBHandler, search_dict):
    existing = handler.search_items(search_dict)
    existing = existing[0] if existing else None
    if existing and "id" in existing.keys():
        return existing["id"]
    return None


def _create_employee(store_id: str, user_id: str):
    table = "Employee-2iph2dahajadpnro5xkxcbveoq-staging"
    handler = DynamoDBHandler(table)

    existing_id = _checkExistingID(handler, {"storeID": store_id, "userID": user_id})
    if existing_id:
        return existing_id

    employee = {
        "userID": user_id,
        "storeID": store_id,
        "role": "EMPLOYEE",
        "isResigned": False,
        "isPrimaryStore": True,
    }

    try:
        return handler.save_item(employee)
    except DynamoDBOperationError as e:
        print("DynamoDB Save Error on create_employee: ")
        print(e)
        return None


def _create_extra_event(employee_id: str, ical_link: str):
    table = "ExtraEvent-2iph2dahajadpnro5xkxcbveoq-staging"
    handler = DynamoDBHandler(table)
    date = datetime.now()

    existing_id = _checkExistingID(
        handler, {"employeeID": employee_id, "downloadableURL": ical_link}
    )
    if existing_id:
        return existing_id

    item = {
        "employeeID": employee_id,
        "extraBookingService": "PLANITY",
        "downloadableURL": ical_link,
        "extraDataType": "ICS",
        "lastUpdatedAt": date.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }
    try:
        return handler.save_item(item)
    except DynamoDBOperationError as e:
        print("DynamoDB Save Error on create_extra_event: ")
        print(e)
        return None
