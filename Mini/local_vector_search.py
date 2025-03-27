


import json
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

# Load the saved embeddings
with open("data/embedded_entities.json", "r", encoding="utf-8") as f:
    records = json.load(f)

# Convert to numpy matrix
texts = [r["text"] for r in records]
ids = [r["id"] for r in records]
vectors = np.array([r["embedding"] for r in records])

# Function to search similar entities
def search_similar(query_index, top_k=3):
    query_vec = vectors[query_index].reshape(1, -1)
    similarities = cosine_similarity(query_vec, vectors)[0]
    top_indices = similarities.argsort()[::-1][1:top_k+1]  # skip self-match

    print(f"\n🔎 Query: {texts[query_index][:100]}...\n")
    for i in top_indices:
        print(f"→ {ids[i]} (score: {similarities[i]:.4f})")
        print(f"   {texts[i][:120]}...\n")

# Try it
search_similar(query_index=0, top_k=2)


