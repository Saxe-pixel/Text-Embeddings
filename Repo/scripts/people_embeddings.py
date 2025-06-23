#!/usr/bin/env python3
"""Embed texts and store them in an HDF5 file."""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import h5py
import numpy as np
import torch
from transformers import AutoTokenizer, AutoModel

MODEL_NAME = "jinaai/jina-embeddings-v3"
TABLE_NAME = "texts"
EMB_DIM = 1024
BATCH_SIZE = 32

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "qid_texts_wo_clean.db"
DEFAULT_BD = BASE / "birthdays_clean.json"
DEFAULT_OUT = BASE / "people_embeddings.h5"


def load_model():
    """Load the embedding model."""
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model = AutoModel.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model.eval()
    return tokenizer, model


def embed_batch(texts: list[str], tokenizer, model) -> np.ndarray:
    """Return embeddings for a batch of texts."""
    encoded = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    with torch.no_grad():
        output = model(**encoded)
    token_embeddings = output.last_hidden_state
    mask = encoded["attention_mask"].unsqueeze(-1).expand(token_embeddings.size()).float()
    summed = torch.sum(token_embeddings * mask, dim=1)
    counts = torch.clamp(mask.sum(dim=1), min=1e-9)
    emb = summed / counts
    return emb.cpu().numpy()


def main(db_path: Path, birthday_path: Path, out_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    n_rows = cur.fetchone()[0]
    cur.execute(f"SELECT MAX(LENGTH(qid)) FROM {TABLE_NAME}")
    max_qid_len = cur.fetchone()[0]

    with open(birthday_path, "r", encoding="utf-8") as f:
        birthdays = json.load(f)

    h5 = h5py.File(out_path, "w")
    emb_ds = h5.create_dataset(
        "embeddings",
        shape=(n_rows, EMB_DIM),
        dtype="float32",
        chunks=(128, EMB_DIM),
        compression="lzf",
    )
    qid_ds = h5.create_dataset(
        "qids",
        shape=(n_rows,),
        dtype=h5py.string_dtype("ascii", length=max_qid_len),
    )
    dob_ds = h5.create_dataset(
        "dob",
        shape=(n_rows,),
        dtype=h5py.string_dtype("ascii", length=10),
    )

    tokenizer, model = load_model()

    cur.execute(f"SELECT qid, text FROM {TABLE_NAME}")
    idx = 0
    while True:
        rows = cur.fetchmany(BATCH_SIZE)
        if not rows:
            break
        qids = [r[0] for r in rows]
        texts = [r[1] for r in rows]
        embeddings = embed_batch(texts, tokenizer, model)

        end = idx + len(rows)
        emb_ds[idx:end] = embeddings
        qid_ds[idx:end] = qids
        dob_ds[idx:end] = [birthdays.get(qid, "") for qid in qids]
        idx = end

    h5.close()
    conn.close()
    print(f"\u2705 Wrote {idx} entries to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embed texts from a SQLite table")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--birthdays", type=Path, default=DEFAULT_BD, help="Path to birthdays JSON")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output HDF5 file")
    args = parser.parse_args()
    main(args.db, args.birthdays, args.out)
