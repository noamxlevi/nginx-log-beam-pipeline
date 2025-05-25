# Dataflow Pipeline

## Overview

This project implements a data pipeline that reads a monthly Nginx log file from a Google Cloud Storage bucket, processes the data using Apache Beam and Dataflow, and writes the results to a BigQuery table to gain insight into long-term trends.

The pipeline performs various transformations and aggregations on the log data, providing insights such as total requests, unique IP addresses, frequent request paths, and status codes.

## Features

- **Data Ingestion**: Reads log files from Google Cloud Storage.
- **Data Processing**: Utilizes Apache Beam to transform and aggregate log data.
- **Data Output**: Writes processed data to a BigQuery table.
- **Metrics**: 
    - Total requests.
    - Unique IPs.
    - Frequent request paths.
    - Status codes.
    - Frequent referrers.

## Usage

    python3 main.py \
    --project <PROJECT> \
    --region <REGION> \
    --runner <RUNNER> \
    --temp_location <TEMP_FOLDER> \
    --staging_location <STAGING_FOLDER> \
    --source <SOURCE_FILE> \
    --bucket <BUCKET> \
    --dataset <DATASET> \
    --table <TABLE> \
    --date <YYYY/MM> \
    --setup <./setup.py>


The pipeline assumes log files are monthly and log line format:

    <IP> - - [<timestamp>] "<request_method> <request_url> <http_version>" <status_code> <response_size> "<referrer>" "<user_agent>"