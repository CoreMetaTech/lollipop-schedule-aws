import json
import logging
from typing import Dict, Any, Optional
from db import DynamoDBHandler, DynamoDBOperationError

# Constants
TABLE_NAME = "DeviceInfo-2iph2dahajadpnro5xkxcbveoq-staging"
USER_TABLE_NAME = "User-2iph2dahajadpnro5xkxcbveoq-staging"
HTTP_METHOD_POST = "POST"
HTTP_METHOD_GET = "GET"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    if event["httpMethod"] == HTTP_METHOD_POST:
        return _handle_post(event)
    elif event["httpMethod"] == HTTP_METHOD_GET:
        return _handle_get(event)
    else:
        return responseJson(405, message="method_not_allowed")


def _handle_get(event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        query_params = event.get("queryStringParameters", {})
        email = query_params.get("email")
        deviceToken = query_params.get("deviceToken")

        if not email or not deviceToken:
            return responseJson(400, message="missing_required_fields")

        handler = DynamoDBHandler(TABLE_NAME)
        items = handler.search_items({"email": email, "deviceToken": deviceToken})
        if not items or len(items) == 0:
            return responseJson(404, message="device_not_found")

        first_item = items[0]
        response = {
            "isOn": first_item["isOn"],
            "device_token": first_item["deviceToken"],
            "email": first_item["email"],
        }

        return responseJson(200, body=response)
    except DynamoDBOperationError as e:
        logger.error(f"DynamoDB operation error: {e}")
        return responseJson(500, message=f"database_error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return responseJson(500, message="internal_server_error")


def _handle_post(event: Dict[str, Any]) -> Dict[str, Any]:
    try:
        body = _parse_body(event)
        token = body.get("device_token")
        email = body.get("email")
        isOn = body.get("isOn", True)

        if not token or not email:
            return responseJson(400, message="missing_required_fields")

        if not _check_email_valid(email):
            return responseJson(400, message="user_not_found")

        if isOn is not None:
            isOn = bool(isOn)
        else:
            isOn = True

        saved_id = _save_token(token, email, isOn)
        return responseJson(
            200,
            {"email": email, "device_token": token, "isOn": isOn},
        )

    except json.JSONDecodeError:
        return responseJson(400, message="request_body_invalid_json")
    except DynamoDBOperationError as e:
        logger.error(f"DynamoDB operation error: {e}")
        return responseJson(500, message=f"database_error: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return responseJson(500, message="internal_server_error")


def _parse_body(event: Dict[str, Any]) -> Dict[str, Any]:
    body = event.get("body")
    if body:
        return json.loads(body) if isinstance(body, str) else body
    return {}


def responseJson(
    statusCode: int, body: Dict[str, Any] = None, message: str = None
) -> Dict[str, Any]:
    response = {
        "statusCode": statusCode,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
        },
    }

    if body is not None:
        response["body"] = json.dumps(body)
    elif message is not None:
        response["body"] = json.dumps({"message": message})

    return response


def _check_email_valid(email: str) -> bool:
    try:
        handler = DynamoDBHandler(USER_TABLE_NAME)
        items = handler.search_items({"email": email})
        return items is not None
    except DynamoDBOperationError as e:
        logger.error(f"DynamoDB Get Error on _check_email_valid: {e}")
        return False


def _save_token(token: str, email: str, isOn: bool) -> Optional[str]:
    handler = DynamoDBHandler(TABLE_NAME)
    device_info = {"deviceToken": token, "email": email, "isOn": isOn}

    existing_items = handler.search_items({"deviceToken": token, "email": email})
    id = None
    if existing_items:
        id = existing_items[0]["id"]
    try:
        return handler.save_item(device_info, id)
    except DynamoDBOperationError as e:
        logger.error(f"DynamoDB Save Error on _save_token: {e}")
        return None
