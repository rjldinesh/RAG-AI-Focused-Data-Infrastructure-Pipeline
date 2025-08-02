from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
import requests
import feedparser
import urllib.robotparser
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date, when, count
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import udf
import boto3
import uuid

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Scrape Task
def scrape_bbc_tech_feed():
    os.makedirs('scraped_content/bbc_tech', exist_ok=True)

    # Check robots.txt
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url('http://www.bbc.co.uk/robots.txt')
    rp.read()

    # RSS feed URL
    rss_url = 'http://feeds.bbci.co.uk/news/technology/rss.xml'
    user_agent = 'Mozilla/5.0 (compatible; BBCScraperBot/1.0)'

    # Save RSS feed
    try:
        response = requests.get(rss_url, headers={'User-Agent': user_agent}, timeout=10)
        response.raise_for_status()
        with open('scraped_content/bbc_tech/rss_feed.xml', 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info("Saved RSS feed")
    except Exception as e:
        logger.error(f"Failed to fetch RSS feed: {str(e)}")
        raise

    # Parse RSS feed
    feed = feedparser.parse('scraped_content/bbc_tech/rss_feed.xml')
    articles = feed.entries[:100]  # Limit to 100 articles

    # Configure MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='ZPnglo5gVhmWx1iC0FdY',
        aws_secret_access_key='OC2Ol37Mq7jflr4pcDp5H4vLYCGCUZrFMTUinWVK'
    )

    def fetch_article(entry):
        url = entry.link
        if not rp.can_fetch(user_agent, url):
            logger.warning(f"Blocked by robots.txt: {url}")
            return

        for attempt in range(3):
            try:
                time.sleep(1)  # Respectful delay
                response = requests.get(url, headers={'User-Agent': user_agent}, timeout=10)
                response.raise_for_status()

                # Generate unique filename
                article_id = str(uuid.uuid4())
                filename = f'scraped_content/bbc_tech/article_{article_id}.html'

                # Save locally
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)

                # Upload to MinIO
                s3_client.upload_file(
                    filename,
                    'activefence-bucket',
                    f'bbc_tech/land/article_{article_id}.html'
                )
                logger.info(f"Scraped and uploaded: {url}")
                break
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt == 2:
                    logger.error(f"Max retries reached for {url}")

    # Concurrent fetching
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(fetch_article, articles)

# Initialize Spark Session
def get_spark_session():
    return (
        SparkSession.builder
        .appName("BBC Tech Pipeline")

        # Delta configs (extensions still required)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.520")

        # MinIO/S3A configs
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "ZPnglo5gVhmWx1iC0FdY")
        .config("spark.hadoop.fs.s3a.secret.key", "OC2Ol37Mq7jflr4pcDp5H4vLYCGCUZrFMTUinWVK")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")

        .getOrCreate()
    )

# Bronze Task
def process_bronze():
    spark = get_spark_session()
    logging.getLogger('pyspark').setLevel(logging.INFO)
    logging.getLogger('py4j').setLevel(logging.INFO)

    try:
        # Read HTML files
        html_files = spark.read.format("binaryFile") \
            .load("s3a://activefence-bucket/bbc_tech/raw/*.html")

        # Define UDF for metadata extraction
        def extract_metadata(content):
            try:
                soup = BeautifulSoup(content, 'html.parser')
                article_id = urlparse(soup.find('link', rel='canonical')['href']).path.split('/')[-1]
                title = soup.find('h1').text if soup.find('h1') else None
                pub_date = soup.find('time')['datetime'] if soup.find('time') else None
                summary = soup.find('meta', {'name': 'description'})['content'] if soup.find('meta', {'name': 'description'}) else None
                return (article_id, title, pub_date, summary)
            except Exception as e:
                logger.error(f"Error extracting metadata: {str(e)}")
                return (None, None, None, None)


        schema = StructType([
            StructField("article_id", StringType(), True),
            StructField("title", StringType(), True),
            StructField("pub_date", StringType(), True),
            StructField("summary", StringType(), True)
        ])
        extract_udf = udf(extract_metadata, schema)

        # Extract metadata
        metadata_df = html_files.select(
            extract_udf(col("content")).alias("metadata")
        ).select(
            col("metadata.article_id"),
            col("metadata.title"),
            col("metadata.pub_date"),
            col("metadata.summary"),
            lit(datetime.now()).alias("load_timestamp")
        )

        # Save to Delta
        metadata_df.write.format("delta") \
            .mode("overwrite") \
            .save("s3a://activefence-bucket/bbc_tech/bronze")
        logger.info("Bronze layer processing complete")
    except Exception as e:
        logger.error(f"Bronze processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

# Silver Task
def process_silver():
    spark = get_spark_session()
    logging.getLogger('pyspark').setLevel(logging.INFO)
    logging.getLogger('py4j').setLevel(logging.INFO)

    try:
        # Read bronze Delta table
        df = spark.read.format("delta").load("s3a://activefence-bucket/bbc_tech/bronze")

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
        logger.info("Silver layer processing complete")
    except Exception as e:
        logger.error(f"Silver processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

# Gold Task
def process_gold():
    spark = get_spark_session()
    logging.getLogger('pyspark').setLevel(logging.INFO)
    logging.getLogger('py4j').setLevel(logging.INFO)

    try:
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
        logger.error(f"Gold processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

# Define DAG
with DAG(
    dag_id='bbc_tech_pipeline',
    start_date=datetime(2025, 7, 11),
    schedule=None,
    catchup=False
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_bbc_tech',
        python_callable=scrape_bbc_tech_feed
    )

    # Set task dependencies
    scrape_task
