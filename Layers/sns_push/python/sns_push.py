import boto3
import json

# Initialize the SNS client
sns_client = boto3.client("sns")


def get_endpoint_arn(platform_application_arn, token):
    """
    Check if an endpoint exists by listing all endpoints and matching the token.
    This function assumes you have a mechanism to keep track of existing endpoints.
    """
    try:
        response = sns_client.list_endpoints_by_platform_application(
            PlatformApplicationArn=platform_application_arn
        )
        for endpoint in response["Endpoints"]:
            if endpoint["Attributes"]["Token"] == token:
                return endpoint["EndpointArn"]
    except ClientError as e:
        print(f"Error listing endpoints: {e}")
    return None


def create_endpoint(platform_application_arn, token):
    """
    Create a new endpoint if it doesn't exist or update the existing endpoint.
    """
    existing_endpoint_arn = get_endpoint_arn(platform_application_arn, token)

    if existing_endpoint_arn:
        print(f"Endpoint already exists: {existing_endpoint_arn}")
        return existing_endpoint_arn

    try:
        response = sns_client.create_platform_endpoint(
            PlatformApplicationArn=platform_application_arn,
            Token=token,
        )
        return response["EndpointArn"]
    except sns_client.exceptions.EndpointAlreadyExistsException:
        # Handle case where endpoint already exists
        print("Endpoint already exists. Retrieving existing endpoint ARN.")
        return get_endpoint_arn(platform_application_arn, token)
    except ClientError as e:
        print(f"Error creating endpoint: {e}")
        return None


def send_push_notification(endpoint_arn, message, title):
    payload = {
        "default": message,
        "GCM": json.dumps(
            {"notification": {"title": title, "body": message, "sound": "default"}}
        ),
        "APNS": json.dumps(
            {"aps": {"alert": {"title": title, "body": message}, "sound": "default"}}
        ),
    }

    try:
        response = sns_client.publish(
            TargetArn=endpoint_arn, MessageStructure="json", Message=json.dumps(payload)
        )
        return response
    except sns_client.exceptions.EndpointDisabledException:
        print("Endpoint is disabled. Consider recreating the endpoint.")
        return None
    except ClientError as e:
        print(f"Error sending notification: {e}")
        return None
