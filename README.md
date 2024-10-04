# Data Publisher Application

## Overview

This application generates synthetic order data and publishes it to an AWS Kinesis stream for downstream processing. It simulates orders, produces order details, and writes these to an AWS Kinesis stream, making use of PySpark for data processing and AWS for stream delivery. 

The project is managed using **Poetry** for dependency management and packaging. The entire project can be executed in a Poetry shell, ensuring a clean and isolated environment.

## Table of Contents
- [Overview](#overview)
- [Setup](#setup)
- [Running the Application](#running-the-application)
- [Main Components](#main-components)
- [AWS Configuration](#aws-configuration)
- [Logging](#logging)
- [Dependencies](#dependencies)
- [Error Handling](#error-handling)

## Setup

### 1. Install Poetry
First, ensure Poetry is installed. If not, you can install it using the command:
```bash
pip install poetry
```

### 2. Clone the Repository
Clone the project repository to your local machine:
```bash
git clone https://github.com/mgranica/VPR-data_publisher.git
cd https://github.com/mgranica/VPR-data_publisher.git
```

### 3. Install Dependencies
Activate the Poetry shell and install the dependencies:
```bash
poetry shell
poetry install
```

### 4. AWS Credentials
Make sure that your AWS credentials and configurations are properly set up. You can set them up in one of the following ways:

- **Local Development**: In your `~/.aws/credentials` and `~/.aws/config` files.
- **GitHub Actions**: Set your AWS credentials in GitHub Secrets.

## Running the Application

To run the application and start generating synthetic order data, use the following command:
```bash
python main.py --num_orders <number-of-orders>
```

For example, to generate 10 orders:
```bash
python main.py --num_orders 10
```

### Arguments:
- `--num_orders`: Specifies the number of synthetic orders to generate (default is 5).

## Project Overview

This project processes data for clients, products, and packages using PySpark and Delta Lake. It integrates with AWS S3 for storage and uses Delta tables for handling large datasets efficiently.

### `main.py`
This is the entry point of the application, and it ties together the various components for order generation and Kinesis stream publishing.

Key steps:
1. **Setup Logging**: Logs events to the console and a log file (`app.log`).
2. **Load AWS Credentials**: Loads AWS credentials from either local files or environment variables (for GitHub Actions).
3. **Create Spark Session**: Initializes the Spark session to process data using PySpark.
4. **Read DataFrames**: Loads data from specified AWS S3 paths in parquet or delta format.
5. **Generate and Publish Orders**: Generates synthetic order data and sends it to an AWS Kinesis stream using a Kinesis client.

### `functions.py`
This module contains the core functions for:
- **Reading DataFrames**: From AWS S3 paths using the PySpark session.
- **Generating Payloads**: For synthetic order details.
- **Publishing Orders**: Sending generated data to Kinesis streams.

### `schemas.py`
We use predefined schemas for the various datasets, which ensure consistent structure and data integrity.

### Gold Clients Address Schema

The `gold_clients_address_schema` is designed to capture detailed information about clients, including personal details and address information. Below is the schema:

| Field Name     | Data Type             | Description                          |
|----------------|-----------------------|--------------------------------------|
| `client_id`    | String                | Unique identifier for the client     |
| `first_name`   | String                | Client's first name                  |
| `last_name`    | String                | Client's last name                   |
| `email`        | String                | Client's email address               |
| `phone_number` | String                | Client's contact number              |
| `date_of_birth`| Date                  | Client's date of birth               |
| `gender`       | String                | Client's gender                      |
| `occupation`   | String                | Client's occupation                  |
| `created_at`   | Date                  | Timestamp of record creation         |
| `updated_at`   | Date                  | Timestamp of the last update         |
| `status`       | String                | Current status of the client         |
| `address_id`   | String                | Identifier for the address           |
| `neighborhood` | String                | Neighborhood information             |
| `coordinates`  | Array (Double)        | Geographic coordinates [lat, lon]    |
| `road`         | String                | Name of the road                     |
| `house_number` | String                | Client's house number                |
| `suburb`       | String                | Suburb name                          |
| `city_district`| String                | City district                        |
| `state`        | String                | State information                    |
| `postcode`     | String                | Postal code                          |
| `country`      | String                | Country name                         |
| `lat`          | Float                 | Latitude                             |
| `lon`          | Float                 | Longitude                            |

### Gold Products Schema

The `gold_products_schema` describes the products sold and their components. The components are nested within the product record.

| Field Name          | Data Type      | Description                          |
|---------------------|----------------|--------------------------------------|
| `product_id`        | String         | Unique identifier for the product    |
| `name`              | String         | Product name                         |
| `category`          | String         | Category of the product              |
| `url`               | String         | Product URL                          |
| `price`             | Float          | Product price                        |
| `currency`          | String         | Currency of the price                |
| `product_components`| Array (Struct) | Components that make up the product  |
| `package_id`        | String         | Identifier for the package           |
| `subpackage_id`     | Integer        | Identifier for subpackage (if any)   |
| `package_quantity`  | Integer        | Quantity of the package              |

### Gold Packages Schema

The `gold_packages_schema` is used to capture the dimensions and characteristics of different packages.

| Field Name        | Data Type | Description                           |
|-------------------|-----------|---------------------------------------|
| `package_id`      | String    | Unique identifier for the package     |
| `subpackage_id`   | Integer   | Identifier for subpackage (if any)    |
| `name`            | String    | Name of the package                   |
| `width`           | Integer   | Package width                         |
| `height`          | Integer   | Package height                        |
| `length`          | Integer   | Package length                        |
| `weight`          | Float     | Weight of the package                 |
| `volume`          | Integer   | Volume of the package                 |
| `stock_quantity`  | Integer   | Quantity available in stock           |

### `spark_session.py`
This module is responsible for setting up the PySpark session with configurations for connecting to AWS and reading from or writing to S3.

## AWS Configuration

Ensure that your AWS credentials and configuration files are correctly set up. For local development, the `.aws/credentials` and `.aws/config` files will be used.

The application reads from specific paths in an S3 bucket, defined in the configuration. The structure of the bucket includes multiple layers:
- **Raw Data**: Unprocessed raw files.
- **Bronze Data**: Staging data for initial transformations.
- **Silver Data**: Further refined and cleaned data.
- **Gold Data**: Fully refined data ready for consumption.

The Kinesis stream name, region, and bucket name are read from the AWS config file.

## Logging

The application includes comprehensive logging, which writes log messages both to the console and to a file (`app.log`). You can modify the log level in the `setup_logging` function to adjust verbosity. Default log level is set to `INFO`.

## Dependencies

All dependencies are managed by **Poetry** and specified in the `pyproject.toml` file. This project uses:
- **PySpark**: For distributed data processing.
- **Boto3**: To interact with AWS services like Kinesis and S3.
- **ConfigParser**: To handle configuration files.
- **NumPy**: For some data-related operations.

To install dependencies, simply use:
```bash
poetry install
```

## Error Handling

The application includes robust error handling, ensuring that any unexpected issues during order generation, data reading, or interaction with AWS are properly logged and raised.

Common exceptions handled:
- **AWS Credential Issues**: If AWS credentials are missing or invalid.
- **File Read/Write Errors**: When reading or writing data from AWS S3.
- **Kinesis Client Errors**: Issues when interacting with the Kinesis stream.
  
Each error logs a detailed message to help identify the cause.
