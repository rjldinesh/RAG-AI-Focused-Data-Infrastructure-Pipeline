import logging
import requests
import uuid
import chromadb
from chromadb.config import Settings
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from starlette.responses import JSONResponse

# import os
# os.environ["CHROMA_TELEMETRY_OPT_OUT"] = "true"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class QueryRequest(BaseModel):
    query: str
    top_k: int = 2

# Correct REST endpoints
OLLAMA_EMBED_URL    = "http://ollama:11434/api/embeddings"
OLLAMA_GENERATE_URL = "http://ollama:11434/api/generate"

@app.post(
    "/ask",
    response_class=PlainTextResponse,     # return plain text
    summary="RAG-style query over ChromaDB + Ollama"
)
async def ask_question(req: QueryRequest):
    try:
        # 1️⃣ Connect to ChromaDB
        client = chromadb.PersistentClient(
            path="/chroma/chroma",
            settings=Settings(anonymized_telemetry=False)
        )
        col = client.get_or_create_collection(name="embedding_collection")
        logger.info(f"Collection count: {col.count()}")

        # 2️⃣ Embed the query (batch of 1)
        emb_resp = requests.post(
            OLLAMA_EMBED_URL,
            json={"model": "nomic-embed-text", "prompt": req.query}
        )
        emb_resp.raise_for_status()
        data = emb_resp.json()
        if "embedding" in data:
            query_vec = data["embedding"]
        else:
            raise ValueError(f"No embedding returned: {data}")

        # 3️⃣ Retrieve top-k docs
        results = col.query(
            query_embeddings=[query_vec],
            n_results=req.top_k
        )
        print("Raw results from Chroma:", results)  # 🔍 DEBUG
        docs = results["documents"][0]  # List[str]
        print("Retrieved docs:", docs)  # 🔍 DEBUG

        metadata = results["metadatas"][0] if results["metadatas"] else {}

        # 4️⃣ Build RAG prompt
        context = "\n\n".join(docs)
        prompt = (
            f"Use the following context to answer the question.\n\n"
            f"Context:\n{context}\n\n"
            f"Question: {req.query}\n\nAnswer:"
        )

        # 5️⃣ Generate with smollm2
        gen_resp = requests.post(
            OLLAMA_GENERATE_URL,
            json={
                "model": "smollm2:135m",
                "prompt": prompt,
                "max_tokens": 256,
                "stream": False
            }
        )
        gen_resp.raise_for_status()
        answer = gen_resp.json().get("response") or gen_resp.json().get("results", [{}])[0].get("generated")
        if not answer:
            raise ValueError("No text returned from generation")

        # return answer
        return JSONResponse(status_code=200, content={
            "status": "success",
            "message": "Answer generated successfully",
            "answer": answer.strip(),
            "sources": metadata
        })

    except requests.exceptions.RequestException as e:
        logger.error(f"Ollama API error: {e}")
        raise HTTPException(500, f"Ollama API error: {e}")
    except ValueError as e:
        logger.error(f"Data error: {e}")
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.exception("Unexpected error")
        raise HTTPException(500, "Internal server error")
