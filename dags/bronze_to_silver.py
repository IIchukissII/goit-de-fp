import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re



def create_spark_session(app_name="BronzeToSilver"):
    """
    Creates and returns a Spark session.
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session created successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

def clean_text(text):
    """
    Cleans text by removing unwanted characters.
    """
    return re.sub(r"[^a-zA-Z0-9,.\"\']", "", str(text))

def process_table(spark, table_name):
    """
    Processes a single table from the Bronze layer to the Silver layer.
    - Reads Parquet data from Bronze.
    - Cleans text columns.
    - Deduplicates the data.
    - Writes the result to the Silver layer.
    """
    try:
        logger.info(f"Processing table: {table_name}")

        # Read Parquet from Bronze
        df = spark.read.parquet(f"bronze/{table_name}")
        logger.info(f"Read data from bronze/{table_name} successfully. Row count: {df.count()}")

        # Clean text columns
        clean_text_udf = udf(clean_text, StringType())
        for column in df.columns:
            if df.schema[column].dataType == StringType():
                df = df.withColumn(column, clean_text_udf(df[column]))
        logger.info(f"Cleaned text columns for table: {table_name}")

        # Deduplication
        df = df.dropDuplicates()
        logger.info(f"Deduplicated data for table: {table_name}. Row count after deduplication: {df.count()}")

        # Write to Silver
        df.write.mode("overwrite").parquet(f"silver/{table_name}")
        logger.info(f"Data written to silver/{table_name} successfully.")
    except Exception as e:
        logger.error(f"Error processing table {table_name}: {e}")
        raise

def main():
    """
    Main function to execute the Bronze to Silver pipeline.
    """
    logger.info("Starting Bronze to Silver pipeline...")

    spark = create_spark_session()

    tables = ["athlete_bio", "athlete_event_results"]
    for table in tables:
        process_table(spark, table)

    spark.stop()
    logger.info("Bronze to Silver pipeline completed successfully.")

if __name__ == "__main__":
    main()
