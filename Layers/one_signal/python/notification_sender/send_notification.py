import os
from onesignal_sdk.client import Client
from onesignal_sdk.error import OneSignalHTTPError


class OneSignalNotificationSender:
    def __init__(self):
        # Initialize the OneSignal client with environment variables for app ID and API key
        try:
            app_id = "885d0c68-ea20-4654-a5f8-e43f0ec52a15"
            api_key = "NGIwYWM2NGYtMmQwMy00MWRkLWE2MTktZTg1MjhjYjZmNmM3"

            # Initialize the OneSignal Client
            self.client = Client(app_id=app_id, rest_api_key=api_key)
        except NameError:
            print(
                "OneSignal Client couldn't be initialized. Make sure the SDK is installed."
            )
            self.client = None

    def send_notification(self, message, fr_message=None, segments=["All"]):
        # Prepare the notification payload
        fr_message = message if fr_message is None else fr_message
        notification = {
            "contents": {"en": message, "fr": fr_message},
            "included_segments": segments,
        }

        try:
            # Send the notification
            response = self.client.send_notification(notification)
            print("Notification sent successfully:", response)
            return True
        except Exception as e:
            print("Error sending notification:", str(e))
            raise e

    def send_notification_by_external_ids(self, external_ids, message, fr_message=None):
        if self.client is None:
            print("OneSignal client is not initialized. Cannot send notification.")
            return None

        if fr_message is None:
            fr_message = message

        notification = {
            "contents": {"en": message, "fr": fr_message},
            "included_segments": ["Subscribed Users"],
            "include_external_user_ids": external_ids,
        }

        try:
            response = self.client.send_notification(notification)
            print(f"Full API Response: {response.body}")
            return response
        except OneSignalHTTPError as e:
            print(f"OneSignal HTTP Error: {e}")
            print(f"HTTP Status Code: {e.status_code}")
            print(f"HTTP Response: {e.http_response}")
            return None
        except Exception as e:
            print(f"Unexpected error: {str(e)}")
            raise e


if __name__ == "__main__":
    sender = OneSignalNotificationSender()
    response = sender.send_notification_by_external_ids(
        ["Employee2@coremeta.tech"],
        "Test message in English",
        "Message de test en français",
    )
