import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO configurations
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'ZPnglo5gVhmWx1iC0FdY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'OC2Ol37Mq7jflr4pcDp5H4vLYCGCUZrFMTUinWVK')


def create_spark_session() -> SparkSession:
    """Create and configure Spark session with MinIO settings."""
    try:
        spark = (SparkSession.builder
                 .appName("MinIOProcessingJob")
                 .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                 .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                 .getOrCreate())
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Failed to create Spark session: {str(e)}")
        raise


def main():
    """Main function to process data with Spark."""
    try:
        # Initialize Spark session
        spark = create_spark_session()

        # Read silver Delta table
        df = spark.read.format("delta").load("s3a://activefence-bucket/bbc_tech/silver")

        # Aggregate data
        agg_df = df.groupBy("pub_date").agg(
            count("*").alias("article_count"),
            count(when(col("title") != "Unknown Title", 1)).alias("valid_title_count")
        )

        # Save to Delta
        agg_df.write.format("delta") \
            .mode("overwrite") \
            .save("s3a://activefence-bucket/bbc_tech/gold")
        logger.info("Gold layer processing complete")

    except Exception as e:
        logger.error(f"Error in Spark job: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
