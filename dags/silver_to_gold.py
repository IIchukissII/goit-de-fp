import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, col
from pyspark.sql.types import FloatType



def create_spark_session(app_name="SilverToGold"):
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


def read_silver_tables(spark):
    """
    Reads the Silver tables into DataFrames.
    """
    try:
        logger.info("Reading silver tables...")
        bio_df = spark.read.parquet("silver/athlete_bio")
        event_df = spark.read.parquet("silver/athlete_event_results")
        logger.info("Silver tables loaded successfully.")
        return bio_df, event_df
    except Exception as e:
        logger.error(f"Error reading silver tables: {e}")
        raise


def preprocess_bio_data(bio_df):
    """
    Casts weight and height columns in the bio DataFrame to FloatType.
    """
    try:
        logger.info("Casting weight and height columns to FloatType...")
        return bio_df.withColumn("weight", col("weight").cast(FloatType())).withColumn(
            "height", col("height").cast(FloatType())
        )
    except Exception as e:
        logger.error(f"Error casting columns: {e}")
        raise


def perform_join_and_aggregate(bio_df, event_df):
    """
    Joins the bio and event DataFrames on athlete_id and performs aggregation.
    Resolves ambiguity by renaming or dropping duplicate columns.
    """
    try:
        logger.info("Performing join on athlete_id...")

        # Rename conflicting column in event_df
        event_df = event_df.withColumnRenamed("country_noc", "event_country_noc")

        joined_df = bio_df.join(event_df, "athlete_id")
        logger.info("Join completed successfully.")

        logger.info("Aggregating data by sport, medal, sex, and country_noc...")
        agg_df = (
            joined_df.groupBy("sport", "medal", "sex", col("country_noc").alias("bio_country_noc"))
            .agg(avg("height").alias("avg_height"), avg("weight").alias("avg_weight"))
            .withColumn("timestamp", current_timestamp())
        )
        logger.info("Aggregation completed successfully.")
        return agg_df
    except Exception as e:
        logger.error(f"Error during join or aggregation: {e}")
        raise


def write_to_gold(agg_df):
    """
    Writes the aggregated DataFrame to the Gold layer.
    """
    try:
        logger.info("Writing aggregated data to gold/avg_stats...")
        agg_df.write.mode("overwrite").parquet("gold/avg_stats")
        logger.info("Data written to gold/avg_stats successfully.")
    except Exception as e:
        logger.error(f"Error writing data to Gold: {e}")
        raise


def main():
    """
    Main function to execute the Silver to Gold pipeline.
    """
    try:
        logger.info("Starting Silver to Gold pipeline...")
        spark = create_spark_session()

        bio_df, event_df = read_silver_tables(spark)
        bio_df = preprocess_bio_data(bio_df)
        agg_df = perform_join_and_aggregate(bio_df, event_df)
        write_to_gold(agg_df)

        spark.stop()
        logger.info("Spark session stopped successfully.")
        logger.info("Silver to Gold pipeline completed successfully.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        if "spark" in locals():
            spark.stop()
            logger.info("Spark session stopped due to an error.")


if __name__ == "__main__":
    main()
