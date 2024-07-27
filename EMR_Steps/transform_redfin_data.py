from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create a Spark session to enable Spark functionality
spark = SparkSession.builder.appName("RedfinDataAnalysis").getOrCreate()

def transform_date():
    # Define S3 paths for raw and transformed data
    raw_data_s3_bucket = "s3://redfin-data-project-ak/store-raw-data/city_market_tracker.tsv000.gz"
    transform_data_s3_bucket = "s3://redfin-data-project-ak/transformed-zone/redfin_data.parquet"
    
    # Read the Redfin data from the S3 bucket, inferring schema and setting the delimiter
    redfin_data = spark.read.csv(raw_data_s3_bucket, header=True, inferSchema=True, sep= "\t")

    # Select only specific columns needed for the analysis
    df_redfin = redfin_data.select(['period_end', 'period_duration', 'city', 'state', 'property_type',
        'median_sale_price', 'median_ppsf', 'homes_sold', 'inventory', 'months_of_supply', 'median_dom', 'sold_above_list', 'last_updated'])

    # Remove rows with any missing values to ensure data quality
    df_redfin = df_redfin.na.drop()

    # Extract year from 'period_end' and create a new column "period_end_yr"
    df_redfin = df_redfin.withColumn("period_end_yr", year(col("period_end")))

    # Extract month from 'period_end' and create a new column "period_end_month"
    df_redfin = df_redfin.withColumn("period_end_month", month(col("period_end")))

    # Drop 'period_end' and 'last_updated' columns as they are no longer needed
    df_redfin = df_redfin.drop("period_end", "last_updated")

    # Map month numbers to their respective month names for better readability
    df_redfin = df_redfin.withColumn("period_end_month", 
                    when(col("period_end_month") == 1, "January")
                    .when(col("period_end_month") == 2, "February")
                    .when(col("period_end_month") == 3, "March")
                    .when(col("period_end_month") == 4, "April")
                    .when(col("period_end_month") == 5, "May")
                    .when(col("period_end_month") == 6, "June")
                    .when(col("period_end_month") == 7, "July")
                    .when(col("period_end_month") == 8, "August")
                    .when(col("period_end_month") == 9, "September")
                    .when(col("period_end_month") == 10, "October")
                    .when(col("period_end_month") == 11, "November")
                    .when(col("period_end_month") == 12, "December")
                    .otherwise("Unknown")
                    )

    # Write the transformed DataFrame to the S3 bucket in Parquet format, overwriting existing files
    df_redfin.write.mode("overwrite").parquet(transform_data_s3_bucket)

# Execute the data transformation function
transform_date()