import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

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
        logger.info("Starting Spark job...")
        spark = create_spark_session()

        # Read data from MinIO
        logger.info("Reading parquet data from s3a://activefence-bucket/bbc_tech/bronze")
        df = spark.read.format("parquet").load("s3a://activefence-bucket/bbc_tech/bronze")

        logger.info(f"Input dataframe schema:\n{df.printSchema()}")
        row_count = df.count()
        logger.info(f"Number of rows read from bronze layer: {row_count}")

        # Clean data
        cleaned_df = df.filter(col("article_id").isNotNull()) \
            .withColumn("title", when(col("title").isNull(), "Unknown Title").otherwise(col("title"))) \
            .withColumn("pub_date", to_date(col("pub_date"))) \
            .withColumn("summary", when(col("summary").isNull(), "No Summary").otherwise(col("summary"))) \
            .withColumn("load_timestamp", col("load_timestamp"))

        # Save to Delta
        cleaned_df.write.format("delta") \
            .mode("overwrite") \
            .save("s3a://activefence-bucket/bbc_tech/silver")

        logger.info("Data cleaned and loaded to silver layer!")

    except Exception as e:
        logger.error(f"Error in Spark job: {str(e)}")
        raise
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
