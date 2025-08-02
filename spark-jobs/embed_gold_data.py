import logging
import uuid
from datetime import date, datetime

import chromadb
import requests
from chromadb.config import Settings
from pyspark.sql import SparkSession

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("embed_gold_data")

MAX_BATCH_SIZE = 100
ollama_url = "http://ollama:11434/api/embeddings"
logger = logging.getLogger(__name__)


def sanitize_metadata(d: dict) -> dict:
    """
    Convert all metadata values to Bool, Int, Float, or Str.
    - datetime/date → ISO string
    - None           → empty string
    - other types    → str(v)
    """
    clean = {}
    for k, v in d.items():
        if v is None:
            clean[k] = ""                           # no None allowed
        elif isinstance(v, (date, datetime)):
            clean[k] = v.isoformat()               # turn dates into strings
        elif isinstance(v, (bool, int, float)):
            clean[k] = v                           # keep primitives
        else:
            clean[k] = str(v)                     # fallback to string
    return clean

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
    spark = create_spark_session()
    try:
        df = (
            spark.read
                 .format("delta")
                 .load("s3a://activefence-bucket/bbc_tech/silver")
                 .limit(MAX_BATCH_SIZE)
        )
        count = df.count()
        if count == 0:
            logger.info("No data to embed.")
            return

        rows = df.select("article_id", "title", "pub_date", "summary").collect()

        # ChromaDB client (persisted on disk)
        client = chromadb.PersistentClient(
            path="/chroma/chroma",
            settings=Settings(anonymized_telemetry=False)
        )
        col = client.get_or_create_collection(name="embedding_collection")
        print("embedding collections",col.count())  # should equal number of rows you collected

        # Process in batches
        for i in range(0, len(rows), MAX_BATCH_SIZE):
            batch = rows[i: i + MAX_BATCH_SIZE]
            docs = [r["summary"] for r in batch]
            ids = [f"{r['article_id']}_{uuid.uuid4()}" for r in batch]
            metas = [
                sanitize_metadata({
                    "article_id": ids[j],
                    "title": batch[j]["title"],
                    "pub_date": batch[j]["pub_date"]
                })
                for j in range(len(batch))
            ]

            embeddings = []
            for doc in docs:
                resp = requests.post(
                    ollama_url,
                    json={"model": "nomic-embed-text", "prompt": doc}
                )
                if resp.status_code != 200:
                    logger.error("Embedding failed for doc: %.50s", doc)
                    logger.error("Status %d – Body: %s", resp.status_code, resp.text)
                resp.raise_for_status()

                data = resp.json()
                # Pick the correct field out of the response
                if "embedding" in data:
                    vec = data["embedding"]
                elif "embeddings" in data:
                    vec = data["embeddings"][0]
                else:
                    raise ValueError(f"Unexpected embedding response shape: {data}")

                embeddings.append(vec)

            # now push the whole batch into Chroma
            col.add(
                ids=ids,
                documents=docs,
                embeddings=embeddings,
                metadatas=metas
            )
            logger.info("Embedded batch %d (%d docs)", i // MAX_BATCH_SIZE + 1, len(docs))

            logger.info(f"Finished embedding {len(rows)} records.")
            logger.info(f"ChromaDB now has {col.count()} vectors.")

    except Exception:
        logger.exception("Error in embeddingGoldData")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
