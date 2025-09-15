import os, httpx, re
from typing import List, Dict
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer

QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434")
COLL = "industrial_sql_rag"

_embedder = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
_qdrant = QdrantClient(QDRANT_URL)
SQL_BLOCK = re.compile(r"```sql(.*?)```", re.S|re.I)

SYSTEM_PROMPT = """You are a senior SQL engineer for PostgreSQL.
Rules:
- Return ONLY one SELECT query in a ```sql code block.
- Use explicit JOINs shown in the context.
- No DDL/DML (no INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE).
- Use CURRENT_TIMESTAMP & INTERVAL for relative time.
- Add LIMIT 1000 if returning raw rows (not required for aggregates).
- Assume timestamps are stored in UTC.
"""

def retrieve_cards(question: str, k: int = 8) -> List[Dict]:
    v = _embedder.encode([question], normalize_embeddings=True)[0]
    hits = _qdrant.search(collection_name=COLL, query_vector=v.tolist(), limit=k)
    return [h.payload for h in hits]

def build_prompt(question: str, cards: List[Dict]) -> str:
    lines = ["Context:"]
    for c in cards:
        lines.append(f"- {c['text']}")
    lines.append("\nWrite a single SELECT for the user question.")
    lines.append(f"User question: {question}")
    return "\n".join(lines)

def call_ollama(prompt: str, model="llama3.1") -> str:
    data = {"model": model, "prompt": f"{SYSTEM_PROMPT}\n\n{prompt}", "stream": False}
    
    r = httpx.post(f"{OLLAMA_URL}/api/generate", json=data, timeout=120)
    r.raise_for_status()
    return r.json()["response"]

def extract_sql(text: str) -> str:
    m = SQL_BLOCK.search(text)
    return (m.group(1) if m else text).strip()
