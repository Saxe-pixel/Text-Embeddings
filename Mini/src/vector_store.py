

import json
import numpy as np
from cassio.config import set_cassio_config
from cassio.vector import VectorStore
from dotenv import load_dotenv
import os

# Load .env variables
load_dotenv()

ASTRA_DB_APPLICATION_TOKEN = os.getenv("ASTRA_DB_APPLICATION_TOKEN")
ASTRA_DB_ID = os.getenv("ASTRA_DB_ID")
ASTRA_DB_KEYSPACE = os.getenv("ASTRA_DB_KEYSPACE")
COLLECTION_NAME = "wikidata_embeddings"

# Step 1: Configure Cassio
set_cassio_config(
    database_id=ASTRA_DB_ID,
    database_region="us-east1",  # Adjust if you're in a different region
    application_token=ASTRA_DB_APPLICATION_TOKEN,
    keyspace=ASTRA_DB_KEYSPACE
)

# Step 2: Load embedded records
with open("data/embedded_entities.json", "r", encoding="utf-8") as f:
    records = json.load(f)

# Step 3: Prepare data for insertion
documents = [
    {
        "id": record["id"],
        "text": record["text"],
        "embedding": record["embedding"]
    }
    for record in records
]

# Step 4: Connect to Astra Vector Store
vs = VectorStore(
    collection_name=COLLECTION_NAME,
    embedding_dimension=len(documents[0]["embedding"]),
    metric="cosine"  # or "dotproduct", "euclidean"
)

# Step 5: Upload documents
print(f"Uploading {len(documents)} records to Astra DB...")
vs.upsert_many(documents)
print("✅ Upload complete!")

# Step 6: Search (optional example)
def search_similar(query_vector, top_k=3):
    results = vs.similarity_search(query_vector, top_k=top_k)
    print("\n🔍 Top Matches:")
    for result in results:
        print(f"{result['id']} (score: {result['score']:.4f})\n→ {result['text'][:120]}...\n")

# You can test with: search_similar(documents[0]['embedding'])
