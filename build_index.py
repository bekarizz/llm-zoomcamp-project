import os, itertools
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer
from cards import make_table_cards, make_column_cards, make_example_cards

DB = os.getenv("DB_URL", "postgresql+psycopg2://rag:ragpass@localhost:5432/ragdb")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")
COLL = "industrial_sql_rag"

def main():
    docs = make_table_cards() + make_column_cards(DB) + make_example_cards()
    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
    vecs = model.encode([d["text"] for d in docs], normalize_embeddings=True)

    client = QdrantClient(QDRANT_URL)
    if COLL in [c.name for c in client.get_collections().collections]:
        client.delete_collection(COLL)
    client.recreate_collection(COLL, vectors_config=VectorParams(size=len(vecs[0]), distance=Distance.COSINE))
    points = [PointStruct(id=i, vector=vecs[i].tolist(), payload=docs[i]) for i in range(len(docs))]
    client.upsert(COLL, points=points)
    print(f"Indexed {len(points)} cards")

if __name__ == "__main__":
    main()
