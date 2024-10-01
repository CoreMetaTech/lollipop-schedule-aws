import boto3
import uuid
from datetime import datetime
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from typing import Dict, List, Optional, Any


class DynamoDBOperationError(Exception):
    pass


class DynamoDBHandler:

    def __init__(self, table_name):
        self.dynamodb = boto3.resource("dynamodb")
        self.table = self.dynamodb.Table(table_name)

    @staticmethod
    def generate_AWSDateTime(date: Optional[datetime] = None) -> str:
        return (date or datetime.now()).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    def save_item(self, item, id=None) -> str:
        aws_date = self.generate_AWSDateTime()
        item["id"] = id or str(uuid.uuid4())
        item["updatedAt"] = item["createdAt"] = aws_date

        try:
            self.table.put_item(Item=item)
            return item["id"]
        except ClientError as e:
            raise DynamoDBOperationError(f"Error saving item: {e}")
        except Exception as e:
            raise DynamoDBOperationError(f"An unexpected error occurred: {e}")

    def get_item(self, key):
        """
        # Example usage:
        table_name = 'YourTableName'
        handler = DynamoDBHandler(table_name)

        # Define the key of the item you want to retrieve
        item_key = {
            'primaryKey': 'YourPrimaryKeyValue'
        }

        try:
            item = handler.get_item(item_key)
            if item:
                print("Retrieved Item:", item)
            else:
                print("Item not found.")
        except DynamoDBOperationError as e:
            print("DynamoDB Operation Error:", e)
        """
        try:
            response = self.table.get_item(Key=key)
            item = response.get("Item")
            return item
        except ClientError as e:
            raise DynamoDBOperationError(f"Error getting item: {e}")
        except Exception as e:
            raise DynamoDBOperationError(f"An unexpected error occurred: {e}")

    def search_items(self, search_conditions: Dict[str, Any]) -> List[Dict[str, Any]]:
        filter_expression, expression_attribute_names, expression_attribute_values = self._build_filter_expression(search_conditions)
        try:
            response = self.table.scan(
                FilterExpression=filter_expression,
                ExpressionAttributeNames=expression_attribute_names,
                ExpressionAttributeValues=expression_attribute_values,
            )
            return response.get("Items", [])
        except ClientError as e:
            raise DynamoDBOperationError(f"Error searching items: {e}")
        except Exception as e:
            raise DynamoDBOperationError(f"An unexpected error occurred: {e}")

    def delete_item(self, key):
        """
        # Example usage:
        table_name = 'YourTableName'
        handler = DynamoDBHandler(table_name)

        # Define the key of the item you want to delete
        item_key = {
            'primaryKey': 'YourPrimaryKeyValue'
        }

        try:
            response = handler.delete_item(item_key)
            print("Item deleted successfully:", response)
        except DynamoDBOperationError as e:
            print("DynamoDB Operation Error:", e)
        """
        try:
            response = self.table.delete_item(Key=key)
            return response
        except ClientError as e:
            raise DynamoDBOperationError(f"Error deleting item: {e}")
        except Exception as e:
            raise DynamoDBOperationError(f"An unexpected error occurred: {e}")

    def update_item(self, key: dict, update_expressions: dict):
        update_expressions["updatedAt"] = self.generate_AWSDateTime()

        update_expressions_array = []
        for attr in update_expressions.keys():
            update_expressions_array.append(f"{attr} = :{attr}")

        # update_expression = 'SET Attribute1 = :val1, Attribute2 = :val2'
        update_expression = "SET " + ", ".join(update_expressions_array)

        # expression_attribute_values = {':val1': 'NewValue1', ':val2': 'NewValue2'}
        expression_attribute_values = {
            f":{attr}": value for attr, value in update_expressions.items()
        }

        try:
            response = self.table.update_item(
                Key=key,
                UpdateExpression=update_expression.strip(),
                ExpressionAttributeValues=expression_attribute_values,
                ReturnValues="UPDATED_NEW",
            )
            return response.get("Attributes", {})
        except ClientError as e:
            raise DynamoDBOperationError(f"Error updating item: {e}")
        except Exception as e:
            raise DynamoDBOperationError(f"An unexpected error occurred: {e}")

    def get_item_id(self, search_conditions: dict) -> str:
        try:
            items = self.search_items(search_conditions)
            if items:
                return items[0]["id"]
            else:
                return None
        except DynamoDBOperationError as e:
            return None

    def _build_key_condition_expression(self, key_conditions):
        """
        Build a key condition expression string from a list of key conditions.

        Args:
        - key_conditions (list): A list of tuples, where each tuple contains the attribute name,
          operator, and value for a key condition.
        example code:
        ```
        key_conditions = [
            ("GSIAttribute1", "=", "value1"),
            ("GSIAttribute2", "begins_with", "value2")
        ]
        ```

        Returns:
        - str: The constructed key condition expression string.
        - dict: The expression attribute names dictionary.
        - dict: The expression attribute values dictionary.
        """
        key_condition_expression_parts = []
        expression_attribute_names = {}
        expression_attribute_values = {}

        for idx, (attr, op, val) in enumerate(key_conditions, start=1):
            # Construct placeholder names for the attribute names
            attr_name_placeholder = f"#attr{idx}"
            expression_attribute_names[attr_name_placeholder] = attr

            # Construct placeholder names for the attribute values
            val_placeholder = f":val{idx}"
            expression_attribute_values[val_placeholder] = val

            # Construct the key condition expression part for this condition
            key_condition_expression_parts.append(f"#{attr} {op} :val{idx}")

        key_condition_expression = " AND ".join(key_condition_expression_parts)

        return (
            key_condition_expression,
            expression_attribute_names,
            expression_attribute_values,
        )

    def _build_filter_expression(self, attributes):
        """
        Build a filter expression string from a dictionary of attribute names and values.

        Args:
        - attributes (dict): A dictionary containing attribute names as keys and attribute values as values.
        ```
        # Example usage:
        attributes = {
            "attribute1": "value1",
            "attribute2": "value2"
        }

        # output:
        Filter Expression: "#attribute1 = :attribute1 AND #attribute2 = :attribute2"
        Expression Attribute Names: {'#attribute1': 'attribute1', '#attribute2': 'attribute2'}
        Expression Attribute Values: {':attribute1': 'value1', ':attribute2': 'value2'}
        ```

        Returns:
        - str: The constructed filter expression string.
        """
        filter_expression = " AND ".join(
            [f"#{attr} = :{attr}" for attr in attributes.keys()]
        )
        expression_attribute_names = {f"#{attr}": attr for attr in attributes.keys()}
        expression_attribute_values = {
            f":{attr}": value for attr, value in attributes.items()
        }
        return (
            filter_expression,
            expression_attribute_names,
            expression_attribute_values,
        )

    def search_item_betweens(
        self, start_date: str, end_date: str, key: str, value: str
    ):
        """

        Args:
            start_date (str): date str formatted like `20240523T100000`
            end_date (str): date str formatted like `20240523T100000`
        """

        input_format = "%Y%m%dT%H%M%S"
        db_format = "%Y-%m-%dT%H:%M:%S.000Z"

        # Define the date range
        start_datetime = datetime.strptime(start_date, input_format)
        end_datetime = datetime.strptime(end_date, input_format)

        start_date_str = start_datetime.strftime(db_format)
        end_date_str = end_datetime.strftime(db_format)

        try:
            response = self.table.scan(
                FilterExpression=Attr("startDateTime").lte(end_date_str)
                & Attr("endDateTime").gte(start_date_str)
                & Attr(key).eq(value)
            )
            items = response.get("Items", [])
            return items
        except ClientError as e:
            raise DynamoDBOperationError(f"Error searching items: {e}")
        except Exception as e:
            raise DynamoDBOperationError(f"An unexpected error occurred: {e}")

        return response["Items"]
