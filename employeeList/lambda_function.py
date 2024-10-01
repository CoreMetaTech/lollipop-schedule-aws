import io
import json
from db import DynamoDBHandler, DynamoDBOperationError


def lambda_handler(event, context):

    try:
        # Check if parameters are sent in the request
        if (
            "queryStringParameters" in event
            and event["queryStringParameters"] is not None
        ):
            # Extract parameters from the request
            parameters = event["queryStringParameters"]

            # Example: if client sends a parameter named 'key', you can use it to fetch specific data
            if "user_id" in parameters:
                user_id = parameters["user_id"]

                # Extract the item from the response
                employees, primary_role = _employees(user_id)

                return responseJson(200, employees, primary_role)
            else:
                return responseJson(400, message="request_parameters_missed - user_id")
        else:
            return responseJson(400, message="request_parameters_missed")
    except Exception as e:
        print(e)
        return responseJson(500, message="server_error")


def responseJson(statusCode, employees=None, primary_role=None, message=None):
    return {
        "statusCode": statusCode,
        "body": json.dumps(
            {
                "primary_store_role": primary_role,
                "store_employees": employees,
                "message": message,
            }
        ),
    }


def _employees(user_id: str):
    try:
        employee_table = "Employee-2iph2dahajadpnro5xkxcbveoq-staging"
        employee_handler = DynamoDBHandler(employee_table)
        employees = employee_handler.search_items(
            {"userID": user_id, "isResigned": False}
        )

        store_table = "StoreInfo-2iph2dahajadpnro5xkxcbveoq-staging"
        store_handler = DynamoDBHandler(store_table)

        address_table = "Address-2iph2dahajadpnro5xkxcbveoq-staging"
        address_handle = DynamoDBHandler(address_table)

        result = []
        primary_role = None
        for employee in employees:
            store_id = employee.get("storeID")
            is_primary_store = employee.get("isPrimaryStore", False)

            if is_primary_store:
                primary_role = employee.get("role")

            if store_id:
                store = store_handler.get_item({"id": store_id})
                if store:
                    address = None
                    address_id = store.get("addressID")
                    if address_id:
                        address = address_handle.get_item({"id": address_id})
                        address = _formattedAddress(address) if address else None

                    item = {
                        "store_id": store_id,
                        "store_name": store.get("name"),
                        "is_primary_store": is_primary_store,
                        "address": address,
                        "employees": _colleagues(store_id, employee_handler),
                    }
                    result.append(item)

        return result, primary_role
    except DynamoDBOperationError as e:
        print("Error _employees == " + e)
        return None


def _colleagues(store_id, employee_handler):
    try:
        user_table = "User-2iph2dahajadpnro5xkxcbveoq-staging"
        user_handler = DynamoDBHandler(user_table)

        employees = []
        colleagues = employee_handler.search_items({"storeID": store_id})
        for colleague in colleagues:
            user_id = colleague.get("userID")
            employee_id = colleague.get("id")
            employee = {
                "employee_id": employee_id,
                "user_id": user_id,
                "role": colleague.get("role"),
                "name": "Unknown name",
                "avatar": None,
            }
            if user_id:
                try:
                    user = user_handler.get_item({"id": user_id})
                    if user:
                        # Combine first and last name
                        firstname = user.get("firstName", "")
                        lastname = user.get("lastName", "")
                        name = (
                            " ".join(filter(None, [firstname, lastname]))
                            or "Unknown name"
                        )
                        employee["name"] = name
                        employee["avatar"] = user.get("avatar")
                    employees.append(employee)
                except Exception as e:
                    # Handle the error appropriately, e.g., log it
                    print(f"Error fetching user {user_id}: {str(e)}")
                    employees.append(employee)
        return employees
    except DynamoDBOperationError as e:
        print(f"Error _colleagues: {str(e)}")
        return None


def _formattedAddress(address: dict):
    if address is None:
        return None

    return {
        "id": address.get("id", None),
        "line1": address.get("line1", None),
        "line2": address.get("line2", None),
        "line3": address.get("line3", None),
        "city": address.get("city", None),
        "postcode": address.get("postcode", None),
        "country": address.get("country", None),
    }
