

# src/embedder.py
from transformers import AutoTokenizer, AutoModel
import torch

MODEL_NAME = "jinaai/jina-embeddings-v3"

_tokenizer = None
_model = None

def load_model():
    global _tokenizer, _model
    if _model is None or _tokenizer is None:
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
        _model = AutoModel.from_pretrained(MODEL_NAME)
    return _tokenizer, _model

from transformers import AutoTokenizer, AutoModel
import torch

MODEL_NAME = "jinaai/jina-embeddings-v3"

_tokenizer = None
_model = None

def load_model():
    global _tokenizer, _model
    if _model is None or _tokenizer is None:
        _tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
        _model = AutoModel.from_pretrained(MODEL_NAME, trust_remote_code=True)
    return _tokenizer, _model

def embed_text(texts):
    tokenizer, model = load_model()

    if isinstance(texts, str):
        texts = [texts]

    encoded = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    with torch.no_grad():
        model_output = model(**encoded)

    # Mean pooling
    attention_mask = encoded["attention_mask"]
    token_embeddings = model_output.last_hidden_state

    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    sum_embeddings = torch.sum(token_embeddings * input_mask_expanded, 1)
    sum_mask = torch.clamp(input_mask_expanded.sum(1), min=1e-9)

    embeddings = sum_embeddings / sum_mask
    return embeddings.numpy()

