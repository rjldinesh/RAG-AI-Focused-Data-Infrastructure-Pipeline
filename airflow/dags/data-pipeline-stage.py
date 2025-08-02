import logging
import os
import time
import urllib.robotparser
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Optional
from urllib.parse import urlparse

import boto3
import chromadb
import feedparser
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
# from delta.tables import DeltaTable
from bs4 import BeautifulSoup
from chromadb.config import Settings
from pyspark.sql import SparkSession
from datetime import datetime
from openlineage.client.run import RunEvent, RunState, Run, Job
from openlineage.client import OpenLineageClient


# OpenLineage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# MinIO configurations
MINIO_ENDPOINT = os.getenv('MINIO_ENDPOINT', 'http://minio:9000')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'ZPnglo5gVhmWx1iC0FdY')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'OC2Ol37Mq7jflr4pcDp5H4vLYCGCUZrFMTUinWVK')
SPARK_MASTER = os.getenv('SPARK_MASTER', 'spark://spark-master:7077')
BRONZE_TO_SILVER_SPARK_SCRIPT_PATH = '/opt/spark-jobs/bronze_to_silver.py'
SILVER_TO_GOLD_SPARK_SCRIPT_PATH = '/opt/spark-jobs/silver_to_gold.py'
GOLD_TO_VECTOR_EMBEDDING ="/opt/spark-jobs/embed_gold_data.py"



client = OpenLineageClient.from_environment()
def emit_lineage_event(job_name: str, namespace: str = "airflow"):
    # 1. Create a unique runId
    run_id = str(uuid.uuid4())

    # 2. Build and emit the event
    event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.utcnow().isoformat() + "Z",
        run=Run(runId=run_id),
        job=Job(namespace=namespace, name=job_name),
        producer="https://github.com/OpenLineage/OpenLineage"
    )
    client.emit(event)

# Scrape Task
def scrape_bbc_tech_feed():
    emit_lineage_event("scrape_bbc_tech")
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
    articles = feed.entries[:10000]
    
    # Configure MinIO client
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
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
                    f'bbc_tech/raw/article_{article_id}.html'
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

def extract_metadata(content: bytes, file_key: str) -> Optional[Dict]:
    """Extract metadata from HTML content."""
    try:
        soup = BeautifulSoup(content, 'html.parser')
        article_id = urlparse(soup.find('link', rel='canonical')['href']).path.split('/')[-1] if soup.find('link', rel='canonical') else None
        title = soup.find('h1').text if soup.find('h1') else None
        pub_date = soup.find('time')['datetime'] if soup.find('time') else None
        summary = soup.find('meta', {'name': 'description'})['content'] if soup.find('meta', {'name': 'description'}) else None
        return {
            'article_id': article_id,
            'title': title,
            'pub_date': pub_date,
            'summary': summary,
            'load_timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Error extracting metadata from {file_key}: {str(e)}")
        return None

def download_file(s3_client, bucket_name: str, file_key: str) -> tuple:
    """Download a single file from MinIO and return its content and key."""
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        return file_key, response['Body'].read()
    except Exception as e:
        logger.error(f"Error downloading {file_key}: {str(e)}")
        return file_key, None

def process_bronze(**kwargs):
    """Process HTML files from MinIO and save metadata to Delta table."""
    try:
        emit_lineage_event("process_bronze")
        # Initialize MinIO client
        s3_client = boto3.client(
            's3',
            endpoint_url=os.getenv("MINIO_ENDPOINT", MINIO_ENDPOINT),
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY
        )
        bucket_name = "activefence-bucket"
        prefix = "bbc_tech/raw/"
        output_path = "s3a://activefence-bucket/bbc_tech/bronze"
        max_workers = 10
        storage_options = {
            # "AWS_ENDPOINT_URL": os.getenv("MINIO_ENDPOINT", MINIO_ENDPOINT),
            "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
            "allow_unsafe_rename": "true",
            "AWS_S3_ENDPOINT": MINIO_ENDPOINT
        }

        # List HTML files in MinIO bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        if 'Contents' not in response:
            logger.info("No HTML files found")
            return

        # Filter HTML files
        html_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.html')]
        if not html_files:
            logger.info("No HTML files found")
            return

        # Download all files concurrently
        metadata_list = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {
                executor.submit(download_file, s3_client, bucket_name, key): key
                for key in html_files
            }
            file_contents = []
            for future in as_completed(future_to_key):
                file_key, content = future.result()
                if content:
                    file_contents.append((file_key, content))

        # Process all files concurrently for metadata extraction
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_key = {
                executor.submit(extract_metadata, content, file_key): file_key
                for file_key, content in file_contents
            }
            for future in as_completed(future_to_key):
                metadata = future.result()
                if metadata:
                    metadata_list.append(metadata)

        if not metadata_list:
            logger.info("No metadata extracted")
            return

        # Create DataFrame
        df = pd.DataFrame(metadata_list)
        # Ensure it's a datetime
        df["load_timestamp"] = pd.to_datetime(df["load_timestamp"])

        # Create 'year_month' column in 'YYYY-MM' format
        df["year_month"] = df["load_timestamp"].dt.strftime("%Y-%m")
        df["load_timestamp"] = df["load_timestamp"].astype("datetime64[ms]")
        logger.info(f"Columns : {df.head()}")

        # Set up S3FileSystem for MinIO
        s3 = s3fs.S3FileSystem(
            anon=False,
            key=MINIO_ACCESS_KEY,
            secret=MINIO_SECRET_KEY,
            client_kwargs={'endpoint_url': MINIO_ENDPOINT}
        )

        # Write DataFrame to multiple Parquet files, partitioned by 'col2'
        df.to_parquet(
            output_path,
            filesystem=s3,
            index=False,
            partition_cols=['year_month'],  # Partition by 'col2'
            engine='pyarrow'
        )
        logger.info("Bronze layer processing complete")

    except Exception as e:
        logger.error(f"Bronze processing failed: {str(e)}")
        raise



def embed_gold_data(**kwargs):

    emit_lineage_event("embed_gold_data")

    # Set environment variables needed for Java/Spark
    os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-17-openjdk-arm64"
    os.environ["PATH"] = f"/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/spark/bin:{os.environ.get('PATH', '')}"

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("EmbedGoldData") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", f"{MINIO_ACCESS_KEY}") \
        .config("spark.hadoop.fs.s3a.secret.key", f"{MINIO_SECRET_KEY}") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages",
                "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    logger.debug(f"Spark session started: {spark}")

    chroma_client = chromadb.PersistentClient(path="/chroma/chroma", settings=Settings(anonymized_telemetry=False))
    collection = chroma_client.get_or_create_collection(name="embedding_collection")

    ollama_url = "http://ollama:11434/api/embed"

    try:
        gold_bucket = "s3a://activefence-bucket/bbc_tech/silver"
        df = spark.read.format("delta").load(gold_bucket)
        logger.debug(f"Sample DF row: {df.head()}")

        rows = df.select("article_id", "title", "pub_date", "summary").collect()
        if not rows:
            logger.info("No new data to process.")
            return

        documents = []
        metadatas = []
        ids = []

        for row in rows:
            article_id = str(row["article_id"]) + str(uuid.uuid4())
            title = str(row["title"])
            pub_date = str(row["pub_date"])
            content = row["summary"]

            # Generate embedding via Ollama
            payload = {"model": "nomic-embed-text", "input": content}
            response = requests.post(ollama_url, json=payload)
            response.raise_for_status()
            embeddings = response.json()['embeddings'][0]

            documents.append(content)
            metadatas.append({"article_id": article_id, "title": title, "pub_date": pub_date})
            ids.append(article_id)

        collection.add(
            ids=ids,
            documents=documents,
            embeddings=[embeddings for _ in ids],
            metadatas=metadatas
        )
        logger.info(f"Embedded {len(documents)} records")
        logger.info(f"Collection record count: {collection.count()}")

    except Exception as e:
        logger.error(f"Error processing Delta table: {str(e)}")
        raise
    finally:
        spark.stop()
        chroma_client.persist()



# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    'data-pipeline-stage_dag',
    default_args=default_args,
    description='Process HTML files from MinIO and save metadata to Delta table',
    schedule=None,
    start_date=datetime(2025, 7, 13),
    catchup=False,
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_ingest_bbc_tech_articles',
        python_callable=scrape_bbc_tech_feed
    )
    
    # Define the PythonOperator task
    process_bronze_task = PythonOperator(
        task_id='raw_html_to_parquet_task',
        python_callable=process_bronze
    )

    bronze_to_silver_spark_task = SparkSubmitOperator(
        task_id='bronze_to_silver_spark_task',
        application=BRONZE_TO_SILVER_SPARK_SCRIPT_PATH,
        conn_id='spark_submit_default',
        spark_binary='/opt/spark/bin/spark-submit',
        # spark_binary='spark-submit',
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0",
        application_args=[],
        conf={
            'spark.submit.deployMode': 'client',
            'spark.hadoop.fs.s3a.endpoint': 'http://project-v2-minio-1:9000',
            'spark.hadoop.fs.s3a.access.key': MINIO_ACCESS_KEY,
            'spark.hadoop.fs.s3a.secret.key': MINIO_SECRET_KEY,
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
            'spark.executor.cores': '2',
        },
       env_vars={
        "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
        "PATH": f"/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/spark/bin:{os.environ['PATH']}"
    },
        executor_cores=2,
        executor_memory='2g',
        driver_memory='1g',
        name='bronze_to_silver_job',
        verbose=True,
    )

    silver_to_gold_spark_task = SparkSubmitOperator(
        task_id='silver_to_gold_spark_task',
        application=SILVER_TO_GOLD_SPARK_SCRIPT_PATH,
        conn_id='spark_submit_default',
        spark_binary='/opt/spark/bin/spark-submit',
        # spark_binary='spark-submit',
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0",
        application_args=[],
        conf={
            'spark.submit.deployMode': 'client',
            'spark.hadoop.fs.s3a.endpoint': 'http://project-v2-minio-1:9000',
            'spark.hadoop.fs.s3a.access.key': MINIO_ACCESS_KEY,
            'spark.hadoop.fs.s3a.secret.key': MINIO_SECRET_KEY,
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '1g',
            'spark.executor.cores': '2',
        },
        env_vars={
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
            "PATH": f"/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/spark/bin:{os.environ['PATH']}"
        },
        executor_cores=2,
        executor_memory='2g',
        driver_memory='1g',
        name='silver_to_gold_job',
        verbose=True,
    )
    #
    embed_to_chromadb_task = SparkSubmitOperator(
        task_id="embed_gold_data_to_chromadb",
        application=GOLD_TO_VECTOR_EMBEDDING,
        name="embed_gold_data_job",
        conn_id="spark_submit_default",
        spark_binary='/opt/spark/bin/spark-submit',
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.520,org.apache.spark:spark-avro_2.12:3.5.0,io.delta:delta-spark_2.12:3.1.0",
        verbose=True,
        conf={
            'spark.submit.deployMode': 'client',
            'spark.hadoop.fs.s3a.endpoint': 'http://project-v2-minio-1:9000',
            'spark.hadoop.fs.s3a.access.key': MINIO_ACCESS_KEY,
            'spark.hadoop.fs.s3a.secret.key': MINIO_SECRET_KEY,
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.executor.memory': '4g',
            'spark.driver.memory': '2g',
            'spark.executor.cores': '1',
            'spark.executorEnv.AWS_ACCESS_KEY_ID': MINIO_ACCESS_KEY,
            'spark.executorEnv.AWS_SECRET_ACCESS_KEY': MINIO_SECRET_KEY,
        },
        application_args=[],
        env_vars={
            "JAVA_HOME": "/usr/lib/jvm/java-17-openjdk-arm64",
            "PATH": f"/usr/lib/jvm/java-17-openjdk-arm64/bin:/opt/spark/bin:{os.environ['PATH']}"
        },
        executor_cores=1,
        executor_memory='4g',
        driver_memory='2g'
    )

# Set task dependencies
scrape_task >>  process_bronze_task >> bronze_to_silver_spark_task >> silver_to_gold_spark_task >> embed_to_chromadb_task