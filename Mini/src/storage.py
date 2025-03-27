

import json

def save_embeddings(path, data):
    """
    Save a list of dicts with keys: id, text, embedding
    to a JSON file at the given path.
    """
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

