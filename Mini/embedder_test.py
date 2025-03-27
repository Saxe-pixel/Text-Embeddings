


from src.data_loader import load_wikidata5m_text
from src.embedder import embed_text
from src.storage import save_embeddings

file_path = "/Users/Saxe/Desktop/GitHub/Text-Embeddings/Mini/data/wikidata5m_text.txt.nosync.txt"
output_path = "data/embedded_entities.json"

# Load text data
data = load_wikidata5m_text(file_path, max_lines=3)
texts = [text for _, text in data]
ids = [eid for eid, _ in data]

# Embed
vectors = embed_text(texts)

# Package + save
records = []
for eid, text, vector in zip(ids, texts, vectors):
    records.append({
        "id": eid,
        "text": text,
        "embedding": vector.tolist()
    })

save_embeddings(output_path, records)
print(f"✅ Saved {len(records)} embeddings to {output_path}")

