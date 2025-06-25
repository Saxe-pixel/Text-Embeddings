#!/usr/bin/env python3
"""Embed texts and store them in an HDF5 file, with MPS/CPU auto‐selection, stable ordering,
progress reporting, fp16 embeddings, and cache clearing to reduce memory."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from pathlib import Path
from datetime import datetime

import h5py
import numpy as np
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel

MODEL_NAME = "jinaai/jina-embeddings-v3"
TABLE_NAME = "texts"
EMB_DIM = 1024
BATCH_SIZE = 8    # keep small to limit per-batch memory

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "qid_texts_wo_clean_test.db"
DEFAULT_BD = BASE / "birthdays_clean_test.json"
DEFAULT_OUT = BASE / "people_embeddings_test.h5"

# choose MPS on Apple Silicon if available, else CPU
if torch.backends.mps.is_available() and torch.backends.mps.is_built():
    DEVICE = torch.device("mps")
    print("🔷 Using MPS backend")
else:
    DEVICE = torch.device("cpu")
    print("⚪️ Using CPU")


def load_model(device: torch.device):
    """Load the embedding model onto the specified device, in half‐precision."""
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model = AutoModel.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model.to(device)
    # reduce model memory
    try:
        model.half()
    except Exception:
        pass
    model.eval()
    return tokenizer, model


def embed_batch(
    texts: list[str],
    tokenizer,
    model,
) -> np.ndarray:
    """Return embeddings for a batch of texts, running on the model's device."""
    encoded = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    # move inputs to same device as model
    encoded = {k: v.to(model.device) for k, v in encoded.items()}
    with torch.no_grad():
        output = model(**encoded)
    token_embeddings = output.last_hidden_state
    mask = encoded["attention_mask"].unsqueeze(-1).expand(token_embeddings.size()).float()
    summed = torch.sum(token_embeddings * mask, dim=1)
    counts = torch.clamp(mask.sum(dim=1), min=1e-9)
    emb = (summed / counts).cpu().numpy().astype(np.float16)  # fp16 output
    # clear intermediate tensors and cache
    del output, token_embeddings, mask, summed, counts, encoded
    if DEVICE.type == "mps":
        torch.mps.empty_cache()
    return emb


def main(db_path: Path, birthday_path: Path, out_path: Path) -> None:
    # connect to SQLite
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    n_rows = cur.fetchone()[0]
    cur.execute(f"SELECT MAX(LENGTH(qid)) FROM {TABLE_NAME}")
    max_qid_len = cur.fetchone()[0]

    # load birthdays JSON
    with open(birthday_path, "r", encoding="utf-8") as f:
        birthdays = json.load(f)

    # create HDF5 file and datasets
    with h5py.File(out_path, "w") as h5:
        emb_ds = h5.create_dataset(
            "embeddings",
            shape=(n_rows, EMB_DIM),
            dtype="f2",                   # half‐precision on disk
            chunks=(64, EMB_DIM),         # smaller chunk to reduce cache bloat
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
        dob_year_ds = h5.create_dataset(
            "dob_year",
            shape=(n_rows,),
            dtype="f4",
        )

        # load tokenizer & model onto DEVICE
        tokenizer, model = load_model(DEVICE)

        # stable ordering by qid (ensures aligned datasets)
        cur.execute(f"SELECT qid, text FROM {TABLE_NAME} ORDER BY qid")

        total_batches = math.ceil(n_rows / BATCH_SIZE)
        idx = 0

        for _ in tqdm(range(total_batches), desc="Embedding batches"):
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break
            qids = [r[0] for r in rows]
            texts = [r[1] for r in rows]
            embeddings = embed_batch(texts, tokenizer, model)

            # prepare DOB strings and numeric years
            dob_strs = [birthdays.get(qid, "") for qid in qids]
            years = []
            for s in dob_strs:
                try:
                    years.append(float(datetime.strptime(s, "%Y-%m-%d").year))
                except Exception:
                    years.append(np.nan)

            end = idx + len(rows)
            emb_ds[idx:end] = embeddings
            qid_ds[idx:end] = qids
            dob_ds[idx:end] = dob_strs
            dob_year_ds[idx:end] = years
            idx = end

            # flush to disk and free any HDF5 cache
            h5.flush()

        print(f"\u2705 Wrote {idx} entries (with fp16 embeddings) to {out_path}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embed texts from a SQLite table")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--birthdays", type=Path, default=DEFAULT_BD, help="Path to birthdays JSON")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output HDF5 file")
    args = parser.parse_args()
    main(args.db, args.birthdays, args.out)
