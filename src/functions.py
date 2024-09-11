import json
import logging
import os
from uuid import uuid4
from datetime import datetime
from datetime import timedelta
import time
import random
import uuid
import logging
import numpy as np
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as f


def read_file(spark: SparkSession, file_path: str, file_type: str, schema: t.StructType, options: dict = None):
    """
    Reads a file into a PySpark DataFrame using a specified schema.

    :param spark: SparkSession object
    :param file_path: Path to the file
    :param file_type: Type of the file (csv, json, parquet, orc)
    :param schema: Schema to enforce
    :param options: Optional dictionary of read options (default is None)
    :return: DataFrame containing the file data
    """
    
    # Validate file_path
    try:
        if not isinstance(file_path, str) or not file_path:
            raise ValueError("Invalid file path provided.")

        if options is None:
            options = {}
        df = (
            spark
            .read
            .format(file_type.lower())
            .options(**options)
            .schema(schema)
            .load(file_path)
        )
        return df
    except Exception as e:
        raise IOError(f"Error reading {file_path}: {str(e)}") 

def write_df(df: DataFrame, file_path: str, file_type: str="parquet", mode: str = "overwrite", options: dict = None):
    """
    Writes a DataFrame to a specified file format with error handling.

    :param df: The DataFrame to write
    :param file_path: Path to write the file
    :param file_type: Type of the file (csv, json, parquet, orc)
    :param mode: Save mode (default is 'overwrite', other options: 'append', 'ignore', 'error')
    :param options: Optional dictionary of write options (default is None)
    """
    
    if options is None:
        options = {}
    
    try:
        # Initialize the writer
        writer = df.write.format(file_type.lower()).mode(mode).options(**options)
        
        # Attempt to save the DataFrame
        writer.save(file_path)
        logging.info(f"DataFrame successfully written to {file_path} as {file_type.upper()}")
    
    except Exception as e:
        logging.error(f"Error writing DataFrame to {file_path} as {file_type.upper()}: {str(e)}")
        raise  # Re-raise the exception after logging it

def generate_order_payload(order_details):
    """
    Generate a payload for an order event.

    :param order_details: Dictionary containing order details.
    :return: Dictionary containing the payload for the order event.
    """
    return {
        "event_id": f"ev-{uuid.uuid4()}",
        "event_type": "ORDER_CREATED",
        "event_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "order_id": f"ord-{uuid.uuid4()}",
        "order_details": order_details
    }

def generate_order_details(df_clients, df_products):
    """
    Generate order details based on client information and item list.

    :param df_clients: DataFrame containing client information.
    :return: Dictionary containing order details.
    """
    current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    item_list = generate_item_list(df_products)
    client_details = select_client_order_details(df_clients)

    return {
        "customer_id": client_details["client_id"],
        "order_timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "order_date": datetime.now().strftime('%Y-%m-%d'),
        "items": item_list,
        "total_amount": generate_item_agg(item_list, "price"),
        "total_weight": generate_item_agg(item_list, "weight"),
        "status": "RECEIVED",
        "destination_address": generate_destination_address_dict(client_details),
        "payment_details": {
            "payment_method": "",
            "payment_status": "",
            "transaction_id": ""
        }
    }

def select_client_order_details(df, primary_key_col="address_id"):
    """
    Select a random row from a DataFrame based on a unique primary key column.
    
    :param df: DataFrame to select from.
    :param primary_key_col: Name of the primary key column. default value: address_id
    :return: DataFrame containing a single randomly selected row.
    """
    # Get a list of all primary key values
    primary_keys = df.select(primary_key_col).rdd.flatMap(lambda x: x).collect()

    # Randomly select one primary key value
    random_primary_key = random.choice(primary_keys)

    # Randomly select one primary key value
    random_primary_key = random.choice(primary_keys)

    # Filter the DataFrame to get the row with the random primary key
    random_row_df = df.filter(f.col(primary_key_col) == random_primary_key)

    # Convert the DataFrame row to dictionary and return
    return random_row_df.first().asDict() if random_row_df else None

def generate_destination_address_dict(clients_dict):
    """
    Filter unnecessary keys from the client's address dictionary.

    :param clients_dict: Dictionary containing client information.
    :return: Dictionary containing filtered address information.
    """
    address_keys = [
        'address_id', 'neighborhood', 'coordinates', 'road', 'house_number',
        'suburb', 'city_district', 'state', 'postcode', 'country', 'lat', 'lon'
    ]
    return {k: v for k, v in clients_dict.items() if k in address_keys}

def generate_item_list(df_products, items=5, quantity=3):
    """
    Generate a list of items with details.
    
    Parameters:
    - items: Number of items to generate details for (default: 5).
    - quantity: Maximum quantity of each item (default: 3).

    Returns:
    - List of dictionaries containing item details.
    """
    return [
        {
            "product_id": item["product_id"], 
            "product_name": item["product_name"], 
            "price": item["price"], 
            "weight": item["weight"],
            "quantity": weighted_random_choice(quantity)
        }
        for item in [ select_client_order_details(df_products, primary_key_col="product_id") for num in range(weighted_random_choice(items))]
    ]

def weighted_random_choice(numbers_len):
    """
    Select a random number from a range starting from 1 with weights based on reciprocal values.

    Parameters:
    - numbers_len: Length of the range of numbers starting from 1.

    Returns:
    - A randomly selected number based on the reciprocal weights.
    """
    # Define numbers range starting from 1 to numbers_len
    numbers = np.arange(1, numbers_len + 1)
    
    # Calculate weights based on reciprocal values
    weights = 1 / numbers
    
    # Ensure the weights sum to 1
    normalized_weights = weights / np.sum(weights)
    
    # Select a random number with the specified weights
    random_number = int(np.random.choice(numbers, p=normalized_weights))
    
    return random_number

def generate_item_agg(items, property_name):
    """
    Generate the aggregate value of a property for a list of items.

    :param items: List of dictionaries containing item details.
    :param property_name: Name of the property to aggregate.
    :return: Aggregate value of the specified property.
    """
    return sum([(item['quantity'] * item[property_name]) for item in items])

def produce_order(payload):
    try:
        # Ensure payload is correctly formatted and partition key is a string
        if 'event_type' not in payload or not isinstance(payload['event_type'], str):
            raise ValueError("Payload must include 'event_type' as a string")
        
        data = json.dumps(payload)
        put_response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=f"{data}\n",
            PartitionKey=payload['event_type']
        )
        
        # Log response details
        logger.info(f"Put record response: {put_response}")
        return put_response
    except Exception as e:
        logger.error(f"Failed to put record to stream: {e}", exc_info=True)
        return None