from prefect import flow, task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, avg, current_timestamp
from pyspark.sql.types import StringType, IntegerType, StructType, StructField
import requests
import os
import logging
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session with proper configuration for Parquet writing."""
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.sql.parquet.enableVectorizedReader", "false")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .config("spark.sql.parquet.mergeSchema", "false")
            .config("spark.sql.parquet.filterPushdown", "true")
            .config("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB
            .getOrCreate())


# Define schema for tables
athlete_bio_schema = StructType([
    StructField("athlete_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("sex", StringType(), True),
    StructField("born", StringType(), True),
    StructField("height", StringType(), True),
    StructField("weight", StringType(), True),
    StructField("country", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("description", StringType(), True),
    StructField("special_notes", StringType(), True)
])

athlete_event_results_schema = StructType([
    StructField("edition", StringType(), True),
    StructField("edition_id", StringType(), True),
    StructField("country_noc", StringType(), True),
    StructField("sport", StringType(), True),
    StructField("event", StringType(), True),
    StructField("result_id", StringType(), True),
    StructField("athlete", StringType(), True),
    StructField("athlete_id", StringType(), True),
    StructField("pos", StringType(), True),
    StructField("medal", StringType(), True),
    StructField("isTeamSport", StringType(), True)
])


@task(retries=3, retry_delay_seconds=30)
def download_data(file: str) -> Optional[str]:
    """Download data from the server and save it locally as a CSV file."""
    url = f"https://ftp.goit.study/neoversity/{file}.csv"
    local_path = f"{file}.csv"

    try:
        logger.info(f"Downloading from {url}")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        with open(local_path, 'wb') as f:
            f.write(response.content)
        logger.info(f"File {local_path} downloaded successfully")
        return local_path
    except Exception as e:
        logger.error(f"Failed to download {file}: {str(e)}")
        raise


@task
def process_landing_to_bronze(table: str) -> Optional[str]:
    """Process a single table: read CSV, convert to Parquet, and save."""
    spark = None
    try:
        spark = get_spark_session("LandingToBronze")
        local_path = f"{table}.csv"
        output_path = f"/tmp/bronze/{table}"

        # Ensure the CSV file exists
        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Input CSV file not found: {local_path}")

        # Create output directory if it doesn't exist
        os.makedirs(output_path, exist_ok=True)

        # Select schema based on table
        if table == "athlete_bio":
            schema = athlete_bio_schema
        elif table == "athlete_event_results":
            schema = athlete_event_results_schema
        else:
            raise ValueError(f"Unknown table: {table}")

        # Read the CSV file
        logger.info(f"Reading CSV file: {local_path}")
        df = (spark.read
              .option("header", "true")
              .option("mode", "DROPMALFORMED")  # Ignore malformed rows
              .schema(schema)  # Apply schema
              .csv(local_path))

        # Validate DataFrame
        if df.rdd.isEmpty():
            raise ValueError(f"No data was read from {local_path}")

        # Write to Parquet with proper partitioning
        logger.info(f"Writing to Parquet: {output_path}")
        (df.coalesce(1)  # Reduce partitions to avoid small files
         .write
         .mode("overwrite")
         .option("compression", "snappy")
         .parquet(output_path))

        # Verify the write was successful
        verification_df = spark.read.parquet(output_path)
        if verification_df.rdd.isEmpty():
            raise ValueError(f"Verification failed: No data in output Parquet file at {output_path}")

        logger.info(f"Successfully processed {table} to bronze layer")
        df.show(truncate=False)
        return output_path

    except Exception as e:
        logger.error(f"Failed to process {table} to bronze layer: {str(e)}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


@task(cache_policy=None)  # Disable caching for this task
def clean_text(df):
    """Clean text columns by trimming whitespace and converting to lowercase."""
    try:
        for column in df.columns:
            if isinstance(df.schema[column].dataType, StringType):
                df = df.withColumn(column, trim(lower(col(column))))
        return df
    except Exception as e:
        logger.error(f"Failed to clean text: {str(e)}")
        raise


@task
def process_bronze_to_silver(table: str) -> Optional[str]:
    """Process a single table: clean text, drop duplicates, and save to silver layer."""
    spark = None
    try:
        spark = get_spark_session("BronzeToSilver")
        input_path = f"/tmp/bronze/{table}"

        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Bronze layer input not found: {input_path}")

        # Read the parquet file from the bronze layer
        df = spark.read.parquet(input_path)

        # Clean the text data
        df = clean_text(df)

        # Drop duplicates
        df = df.dropDuplicates()

        # Show after dropping duplicates
        logger.info(f"Data after dropping duplicates:")
        df.show(truncate=False)

        # Define the output path for the silver layer
        output_path = f"/tmp/silver/{table}"
        os.makedirs(output_path, exist_ok=True)

        # Write the processed DataFrame to the silver layer in Parquet format
        df.coalesce(1).write.mode("overwrite").parquet(output_path)

        logger.info(f"Successfully processed {table} to silver layer")
        return output_path

    except Exception as e:
        logger.error(f"Failed to process {table} to silver layer: {str(e)}")
        raise

    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


@task
def process_silver_to_gold():
    """Process data from silver to gold layer."""
    spark = get_spark_session("SilverToGold")

    # Load tables from the silver layer
    athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
    athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

    # Rename columns to avoid ambiguity when joining
    athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

    # Join the tables on the "athlete_id" column
    joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

    # Calculate average values for each group
    aggregated_df = joined_df.groupBy("sport", "medal", "sex", "country_noc") \
        .agg(
        avg("height").alias("avg_height"),
        avg("weight").alias("avg_weight"),
        current_timestamp().alias("timestamp")
    )

    # Create a directory to save the results in the gold layer
    output_path = "/tmp/gold/avg_stats"
    os.makedirs(output_path, exist_ok=True)

    # Save the processed data in parquet format
    aggregated_df.write.mode("overwrite").parquet(output_path)

    logger.info(f"Data saved to {output_path}")

    # Optional: Re-read and verify
    df = spark.read.parquet(output_path)
    df.show(truncate=False)

    spark.stop()


@flow(retries=1)
def etl_pipeline():
    """End-to-end ETL pipeline from landing to gold."""
    tables = ["athlete_bio", "athlete_event_results"]

    try:
        for table in tables:
            csv_path = download_data(table)
            if csv_path:
                bronze_path = process_landing_to_bronze(table)
                if bronze_path:
                    process_bronze_to_silver(table)

        process_silver_to_gold()
        logger.info("ETL pipeline completed successfully")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    etl_pipeline()
