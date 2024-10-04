import sys
import configparser
import argparse
import os
import time
import logging
import boto3

from spark_session import create_spark_session
from schemas import gold_clients_address_schema, orders_delta_schema
from functions import read_file, generate_order_payload, generate_order_details, produce_order, produce_delta_order


def setup_logging():
    """
    Set up logging configuration.
    """
    logging.basicConfig(
        level=logging.INFO,  # Log level can be adjusted (DEBUG, INFO, WARNING, ERROR)
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),  # Log to console
            logging.FileHandler("app.log", mode='a')  # Optionally log to a file
        ]
    )  

def load_aws_credentials(profile_name="default"):
    # Check the environment flag
    environment = os.getenv('ENVIRONMENT', 'LOCAL')

    if environment == 'GITHUB_ACTIONS':
        # Load credentials from environment variables set in GitHub Actions
        aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("AWS credentials not found in GitHub Actions environment variables.")
            sys.exit(1)

        logging.info("Successfully loaded credentials from GitHub Actions environment.")
    else:
        # Load credentials from the .aws/credentials file (local development)
        try:
            credentials = configparser.ConfigParser()
            credentials.read(os.path.join(os.path.dirname(__file__), '..', '.aws', 'credentials'))
            
            logging.info("Successfully loaded credentials variables from .aws file.")
        except Exception as e:
            logging.error(f"Error loading .aws file: {e}")
            sys.exit(1)

        aws_access_key_id = credentials[profile_name]["aws_access_key_id"]
        aws_secret_access_key = credentials[profile_name]["aws_secret_access_key"]

        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("AWS credentials not found.")
            sys.exit(1)

    return aws_access_key_id, aws_secret_access_key

def load_aws_config():
    """
    Loads AWS configuration settings from the .aws/config file.

    :param profile_name: The profile name in the AWS config file (default: "default").
    :return: The region_name as a string.
    """
    try:
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.dirname(__file__), '..', '.aws', 'config'))
        logging.info("Successfully loaded config variables from .aws file.")

        return config
    except Exception as e:
        logging.error(f"Error loading .aws file: {e}")
        sys.exit(1)

def extract_filepath(config, bucket_name, layer):
    paths = {
        # Define raw paths
        "raw_paths": {
            "address": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["ADDRESS_DATA"]),
            "clients": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["CLIENTS_DATA"]),
            "products": os.path.join(bucket_name, config["paths"]["RAW"], config["raw_data"]["PRODUCTS_DATA"]),
        },
        # Define bronze paths
        "bronze_paths": {
            "address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["ADDRESS_TABLE"]),
            "clients": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["CLIENTS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["BRONZE"], config["table_names"]["PRODUCTS_TABLE"]),
        },
        # Define silver paths
        "silver_paths": {
            "address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["ADDRESS_TABLE"]),
            "clients": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["CLIENTS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["SILVER"], config["table_names"]["PRODUCTS_TABLE"]),
        },
        # Define gold paths
        "gold_paths": {
            "clients_address": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["CLIENTS_ADDRESS_TABLE"]),
            "products": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["PRODUCTS_TABLE"]),
            "packages": os.path.join(bucket_name, config["paths"]["ORDERS"], config["paths"]["GOLD"], config["table_names"]["PACKAGE_TABLE"]),
        }
    }
    
    return paths[layer]

def read_dataframes(spark, config, bucket_name, layer="gold_paths"):
    
    try:
        # Extract file paths
        gold_paths = extract_filepath(config, bucket_name, layer)
        parquet_filetype = config["format"]["parquet"]
        delta_filetype = config["format"]["delta"]

        # Read DataFrames
        df_clients_address = read_file(spark, gold_paths["clients_address"], parquet_filetype, gold_clients_address_schema)
        df_products = read_file(spark, gold_paths["products"], delta_filetype)
        df_packages = read_file(spark, gold_paths["packages"], delta_filetype)

        return df_clients_address, df_products, df_packages
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {str(e)}")


def main():
    # Setup logging
    logger = logging.getLogger(__name__)
    
    parser = argparse.ArgumentParser(description="Generate synthetic orders data and publish the stream.")
    parser.add_argument('--num_orders', type=int, default=5, help='Number of orders to generate')
    args = parser.parse_args()
    # Load credentials and configuration
    aws_access_key_id, aws_secret_access_key = load_aws_credentials()
    aws_config = load_aws_config()

    bucket_name = aws_config["paths"]["BUCKET_NAME"]
    region_name = aws_config["default"]["REGION"]
    # stream_name = aws_config["default"]["STREAM_NAME"]
    orders_stream_path = os.path.join(
        bucket_name, aws_config["paths"]["ORDERS"], aws_config["format"]["delta"], aws_config["default"]["STREAM_NAME"]
    )
    # Create Spark session
    spark = create_spark_session(aws_access_key_id, aws_secret_access_key)
    # Read Dataframes
    df_clients_address ,df_products , df_packages = read_dataframes(spark, aws_config, bucket_name)
    # Generate payload
    for order in range(1, args.num_orders + 1):
        try:
            order_details = generate_order_details(df_clients_address, df_products, df_packages)
            order_payload = generate_order_payload(order_details)
            produce_delta_order(spark, order_payload, orders_delta_schema, orders_stream_path)
            time.sleep(10)  # Consider making 20 a configurable parameter if needed.
        except Exception as e:
            logging.error(f"Error processing order {order}: {e}")
    

if __name__ == "__main__":
    setup_logging()
    main()