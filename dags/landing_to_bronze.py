import logging
import requests
from pyspark.sql import SparkSession


def download_data(local_file_path, logger):
    """
    Downloads a CSV file from the server and saves it locally.
    """
    url = "https://ftp.goit.study/neoversity/"
    downloading_url = f"{url}{local_file_path}.csv"
    logger.info(f"Attempting to download data from: {downloading_url}")

    try:
        response = requests.get(downloading_url, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors

        with open(f"{local_file_path}.csv", "wb") as file:
            file.write(response.content)
        logger.info(f"File {local_file_path}.csv downloaded successfully.")
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download {local_file_path}.csv: {e}")
        raise

def process_table(table, spark, logger):
    """
    Reads a CSV file, processes it with Spark, and writes the data to Parquet.
    """
    try:
        # Read CSV
        logger.info(f"Reading CSV file: {table}.csv")
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(f"{table}.csv")
        )

        # Show schema and a preview of the data
        logger.info(f"Schema for {table}:")
        df.printSchema()
        logger.info(f"Preview of {table} data:")
        df.show(5, truncate=False)

        # Write to Parquet in the Bronze layer
        output_path = f"bronze/{table}"
        logger.info(f"Writing data to: {output_path}")
        df.write.mode("overwrite").parquet(output_path)

        logger.info(f"Table {table} successfully processed and written to Bronze.")
    except Exception as e:
        logger.error(f"Error processing table {table}: {e}")
        raise

def main():
    """
    Main function to orchestrate the Landing to Bronze process.
    """

    # Initialize Spark session
    try:
        spark = SparkSession.builder.appName("LandingToBronze").getOrCreate()
        logger.info("Spark session created successfully.")
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

    # List of tables to process
    tables = ["athlete_bio", "athlete_event_results"]

    # Process each table
    for table in tables:
        try:
            logger.info(f"Processing table: {table}")
            download_data(table, logger)
            process_table(table, spark, logger)
        except Exception as e:
            logger.error(f"Error during processing of {table}: {e}")

    # Stop Spark session
    spark.stop()
    logger.info("Landing to Bronze process completed.")

if __name__ == "__main__":
    main()
