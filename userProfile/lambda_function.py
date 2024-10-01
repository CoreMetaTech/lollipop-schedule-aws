import io
import json
from db import DynamoDBHandler
from db import DynamoDBOperationError

user_table = "User-2iph2dahajadpnro5xkxcbveoq-staging"
address_table = "Address-2iph2dahajadpnro5xkxcbveoq-staging"
employee_table = "Employee-2iph2dahajadpnro5xkxcbveoq-staging"
store_table = "StoreInfo-2iph2dahajadpnro5xkxcbveoq-staging"
opentime_table = "OpenTime-2iph2dahajadpnro5xkxcbveoq-staging"


def lambda_handler(event, context):
    if event["httpMethod"] == "GET":
        return _getProfile(event)
    elif event["httpMethod"] == "POST":
        return _postProfile(event)
    return responseJson(405, message="Method Not Allowed")


def responseJson(statusCode, profile=None, message=None):
    return {
        "statusCode": statusCode,
        "body": json.dumps({"profile": profile, "message": message}),
    }


def _postProfile(event):
    try:
        # auth_user_id = None

        # if "requestContext" in event and "identity" in event["requestContext"]:
        #     auth_user_id = event["requestContext"]["identity"].get("user")

        # # If using a custom authorizer, user ID might be in claims
        # if "requestContext" in event and "authorizer" in event["requestContext"]:
        #     claims = event["requestContext"]["authorizer"].get("claims")
        #     if claims and "sub" in claims:
        #         auth_user_id = claims["sub"]

        # if auth_user_id:
        #     # Parse the body from JSON
        # else:
        #     return responseJson(403, message="not_auth")

        body = event.get("body")
        if body:
            body = json.loads(body)
            user_id = body.get("user_id")
            firstName = body.get("first_name")
            lastName = body.get("last_name")
            phone = body.get("phone")
            primary_store_id = body.get("primary_store_id")
            if user_id:
                if firstName or lastName or phone:
                    _update_profile(user_id, firstName, lastName, phone)
                if primary_store_id:
                    _update_primary_store(user_id, primary_store_id)
                return _profile(user_id)
            else:
                return responseJson(400, message="request_body_missed_user_id")
    except json.JSONDecodeError:
        return responseJson(400, message="request_body_invalid_json")
    except DynamoDBOperationError as e:
        return responseJson(500, message="error: " + str(e))


def _update_profile(user_id, firstName, lastName, phone, primary_store_id):
    dynamodb_client = DynamoDBHandler(user_table)
    attributes = {}
    if firstName != None and len(firstName) > 0:
        attributes["firstName"] = firstName
    if lastName != None and len(lastName) > 0:
        attributes["lastName"] = lastName
    if phone != None and len(phone) > 0:
        attributes["phone"] = phone
    result = dynamodb_client.update_item({"id": user_id}, attributes)
    return result


def _update_primary_store(user_id, primary_store_id):
    dynamodb_client = DynamoDBHandler(employee_table)
    employees = dynamodb_client.search_items({"userID": user_id})

    stores = _getStores(user_id)

    employee_id = None
    for employee in employees:
        store_id = employee.get("storeID", None)
        employee_id = employee.get("id", None)

        if store_id == primary_store_id:
            attributes = {"isPrimaryStore": True}
            dynamodb_client.update_item({"id": employee_id}, attributes)
        else:
            attributes = {"isPrimaryStore": False}
            dynamodb_client.update_item({"id": employee_id}, attributes)


def _getProfile(event):
    query_params = event.get("queryStringParameters", {})
    user_id = query_params.get("user_id")

    if user_id is None:
        return responseJson(400, message="Missing user_id")
    return _profile(user_id)


def _profile(user_id: str):
    try:
        user = _getItem(user_id, user_table)
        if user is None:
            return responseJson(404, message="User not found")

        address = _getItem(user.get("addressID", None), address_table)

        result = {
            "name": {
                "firstName": user.get("firstName", None),
                "lastName": user.get("lastName", None),
            },
            "email": user.get("email", None),
            "phone": user.get("phone", None),
            "avatar": user.get("avatar", None),
            "address": _formattedAddress(address),
            "stores": _getStores(user_id),
        }

        return responseJson(200, result)
    except DynamoDBOperationError as e:
        return responseJson(500, message="userID: " + user_id + ".\n Error:::" + str(e))


def _getStores(user_id: str):
    store = None
    dynamodb_client = DynamoDBHandler(employee_table)
    employees = dynamodb_client.search_items({"userID": user_id})

    stores = []
    for employee in employees:
        isResigned = employee.get("isResigned", False)
        isPrimary = employee.get("isPrimaryStore", False)

        if isResigned:
            continue

        store_id = employee.get("storeID", None)
        employee_role = employee.get("role", None)
        employee_id = employee.get("id", None)
        store = _generateStore(store_id, employee_role, isPrimary, employee_id)
        stores.append(store)
    return stores


def _getItem(id: str, table: str):
    if id is None:
        return None
    dynamodb_client = DynamoDBHandler(table)
    item = dynamodb_client.get_item({"id": id})
    return item


def _generateStore(
    store_id: str, employee_role: str, isPrimary: bool, employee_id: str
):
    if store_id is None:
        return None

    store = _getItem(store_id, store_table)
    if store:
        name = store.get("name", None)
        website = store.get("website", None)
        phone = store.get("phone", None)
        address = _getItem(store.get("addressID", None), address_table)
        opentime = _getItem(store.get("opentimeID", None), opentime_table)
        role = employee_role

        store_info = {
            "id": store_id,
            "name": name,
            "website": website,
            "phone": phone,
            "address": _formattedAddress(address),
            "opentime": _formattedOpentime(opentime),
            "role": role,
            "isPrimary": isPrimary,
            "employee_id": employee_id,
        }
        return store_info
    return None


def _formattedAddress(address: dict):
    if address is None:
        return None

    return {
        "id": address.get("id"),
        "line1": address.get("line1", None),
        "line2": address.get("line2", None),
        "line3": address.get("line3", None),
        "city": address.get("city", None),
        "postcode": address.get("postcode", None),
        "country": address.get("country", None),
    }


def _formattedOpentime(opentime: dict):
    if opentime is None:
        return None

    return {
        "id": opentime.get("id"),
        "monday": opentime.get("monday", None),
        "tuesday": opentime.get("tuesday", None),
        "wednesday": opentime.get("wednesday", None),
        "thursday": opentime.get("thursday", None),
        "friday": opentime.get("friday", None),
        "saturday": opentime.get("saturday", None),
        "sunday": opentime.get("sunday", None),
    }
