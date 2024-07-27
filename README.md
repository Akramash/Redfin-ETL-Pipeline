# Redfin ETL Pipeline

## Overview

This repository contains the ETL (Extract, Transform, Load) pipeline designed to process and analyze Redfin real estate market data. The pipeline leverages Apache Airflow, AWS EMR, PySpark, and Snowflake to automate data ingestion, transformation, and loading processes. The system is capable of integrating with BI tools such as Power BI and AWS QuickSight for data visualization and reporting.

## System Architecture

### Data Flow

1. **Data Ingestion**: Data is fetched from Redfin's public data source and stored in an S3 bucket.
2. **Data Transformation**: Using AWS EMR and PySpark, the data is transformed into a format suitable for analysis.
3. **Data Loading**: Transformed data is loaded into Snowflake for storage and further analysis.
4. **Data Visualization**: Snowflake data is connected to Power BI and AWS QuickSight for visualization and reporting.

### Technologies Used

- **Apache Airflow**: Orchestration and scheduling of ETL jobs.
- **AWS EMR**: Scalable data processing using Spark.
- **PySpark**: Data transformation and analysis.
- **Snowflake**: Data warehousing.
- **Power BI & AWS QuickSight**: Business intelligence and data visualization.

## Detailed Explanation of Key Components

### Airflow DAG (`DAGS/redfin_analytics.py`)

The DAG defines the workflow for the ETL pipeline, including tasks such as creating the EMR cluster, adding Spark steps for data extraction and transformation, and terminating the cluster.

```python
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.dummy_operator import DummyOperator
import boto3
from airflow.providers.amazon.aws.operators.emr import (
    EmrCreateJobFlowOperator, 
    EmrAddStepsOperator,
    EmrTerminateJobFlowOperator
)
from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor, EmrStepSensor

# Configuration, default arguments, and task definitions...
```

### Data Ingestion Script (`EMR_Steps/ingest.sh`)

This script downloads the raw data from Redfin and uploads it to the specified S3 bucket.


```bash
wget -O - https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz | aws s3 cp - s3://redfin-data-project-ak/store-raw-data/city_market_tracker.tsv000.gz
```

### Data Transformation Script (`EMR_Steps/transform_redfin_data.py`)

This PySpark script reads the raw data from S3, performs data cleaning and transformation, and writes the transformed data back to S3 in Parquet format.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("RedfinDataAnalysis").getOrCreate()

def transform_date():
    raw_data_s3_bucket = "s3://redfin-data-project-ak/store-raw-data/city_market_tracker.tsv000.gz"
    transform_data_s3_bucket = "s3://redfin-data-project-ak/transformed-zone/redfin_data.parquet"
    
    redfin_data = spark.read.csv(raw_data_s3_bucket, header=True, inferSchema=True, sep= "\t")
    df_redfin = redfin_data.select([...]).na.drop()
    df_redfin = df_redfin.withColumn("period_end_yr", year(col("period_end"))).withColumn("period_end_month", month(col("period_end"))).drop("period_end", "last_updated")
    df_redfin = df_redfin.withColumn("period_end_month", when(...))
    df_redfin.write.mode("overwrite").parquet(transform_data_s3_bucket)

transform_date()
```

### Snowflake Script (`Snowflake/RealEstate_Script`)

This SQL script sets up the database, schemas, and tables in Snowflake, and creates a Snowpipe for automated data loading from S3.

```SQL
-- Drop and create the database
DROP DATABASE IF EXISTS redfin_database_1;
CREATE DATABASE redfin_database_1;

-- Create schema and table
CREATE SCHEMA redfin_schema;
CREATE OR REPLACE TABLE redfin_database_1.redfin_schema.redfin_table (...);

-- Create Parquet file format and stage
CREATE SCHEMA file_format_schema;
CREATE OR REPLACE FILE FORMAT redfin_database_1.file_format_schema.format_parquet TYPE = 'PARQUET';
CREATE SCHEMA external_stage_schema;
CREATE OR REPLACE STAGE redfin_database_1.external_stage_schema.redfin_ext_stage_parquet URL = 's3://redfin-data-project-ak/transformed-zone/' CREDENTIALS = (aws_key_id='****' aws_secret_key='****') FILE_FORMAT = redfin_database_1.file_format_schema.format_parquet;

-- Create Snowpipe for automated data loading
CREATE OR REPLACE PIPE redfin_database_1.snowpipe_schema.redfin_snowpipe AUTO_INGEST = TRUE AS COPY INTO redfin_database_1.redfin_schema.redfin_table FROM @redfin_database_1.external_stage_schema.redfin_ext_stage_parquet FILE_FORMAT = (TYPE = PARQUET) MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE ON_ERROR = 'SKIP_FILE';
```
### Connecting to Power BI and AWS QuickSight

#### Power BI

Install the Snowflake connector for Power BI.

Use your Snowflake account credentials to connect and import the transformed data for visualization.

#### AWS QuickSight

Set up Snowflake as a data source in AWS QuickSight.

Configure data imports and create interactive dashboards using the transformed data.

### Conclusion
This ETL pipeline provides a robust and scalable solution for processing Redfin real estate data, leveraging a combination of AWS services, PySpark, and Snowflake. The integration with Power BI and AWS QuickSight enables comprehensive data analysis and visualization, facilitating data-driven decision-making.
