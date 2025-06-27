#!/usr/bin/env python3
"""Embed selected texts and store them in an HDF5 file.

This script behaves like ``people_embeddings_death.py`` but embeds only the QIDs
listed in a text file.  The list should contain one QID per line.  By default the
file ``custom_qids.txt`` is looked for inside ``WikiData.nosync/`` (the same
location as the databases).
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel

MODEL_NAME = "jinaai/jina-embeddings-v3"
TABLE_NAME = "texts"
EMB_DIM = 1024
BATCH_SIZE = 4    # keep small to limit per-batch memory

DEFAULT_MAX_EMBEDDINGS = 10000  # limit to avoid infinite runs (if set to None, no limit)

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "qid_texts_wo_m_clean.db"
DEFAULT_BD = BASE / "death_dates_clean.json"
DEFAULT_LIST = BASE / "custom_qids.txt"
DEFAULT_OUT = BASE / "people_embeddings_death_custom.h5"

# choose MPS on Apple Silicon if available, else CPU
if torch.backends.mps.is_available() and torch.backends.mps.is_built():
    DEVICE = torch.device("mps")
    print("🔷 Using MPS backend")
else:
    DEVICE = torch.device("cpu")
    print("⚪️ Using CPU")


def load_model(device: torch.device):
    """Load the embedding model onto the specified device, in half-precision."""
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model = AutoModel.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model.to(device)
    try:
        model.half()
    except Exception:
        pass
    model.eval()
    return tokenizer, model


def embed_batch(texts: list[str], tokenizer, model) -> np.ndarray:
    """Return embeddings for a batch of texts on the model's device."""
    encoded = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    encoded = {k: v.to(model.device) for k, v in encoded.items()}
    with torch.no_grad():
        output = model(**encoded)
    token_embeddings = output.last_hidden_state
    mask = encoded["attention_mask"].unsqueeze(-1).expand(token_embeddings.size()).float()
    summed = torch.sum(token_embeddings * mask, dim=1)
    counts = torch.clamp(mask.sum(dim=1), min=1e-9)
    emb = (summed / counts).cpu().numpy().astype(np.float16)
    del output, token_embeddings, mask, summed, counts, encoded
    if DEVICE.type == "mps":
        torch.mps.empty_cache()
    return emb


def main(
    db_path: Path,
    death_path: Path,
    list_path: Path,
    out_path: Path,
    max_embeddings: int | None = DEFAULT_MAX_EMBEDDINGS,
) -> None:
    """Embed the texts for the given QIDs and store them with dates of death."""
    with open(list_path, "r", encoding="utf-8") as f:
        qid_list = [line.strip() for line in f if line.strip()]

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    all_qids: list[str] = []
    all_texts: list[str] = []
    for qid in qid_list:
        cur.execute(f"SELECT text FROM {TABLE_NAME} WHERE qid = ?", (qid,))
        row = cur.fetchone()
        if row:
            all_qids.append(qid)
            all_texts.append(row[0])

    if max_embeddings:
        all_qids = all_qids[:max_embeddings]
        all_texts = all_texts[:max_embeddings]

    n_rows = len(all_qids)
    max_qid_len = max(len(q) for q in all_qids) if all_qids else 1

    with open(death_path, "r", encoding="utf-8") as f:
        deaths = json.load(f)

    with h5py.File(out_path, "w") as h5:
        emb_ds = h5.create_dataset(
            "embeddings",
            shape=(n_rows, EMB_DIM),
            dtype="f2",
            chunks=(64, EMB_DIM),
            compression="lzf",
        )
        qid_ds = h5.create_dataset(
            "qids",
            shape=(n_rows,),
            dtype=h5py.string_dtype("ascii", length=max_qid_len),
        )
        dod_ds = h5.create_dataset(
            "dod",
            shape=(n_rows,),
            dtype=h5py.string_dtype("ascii", length=10),
        )
        dod_year_ds = h5.create_dataset(
            "dod_year",
            shape=(n_rows,),
            dtype="f4",
        )

        tokenizer, model = load_model(DEVICE)

        total_batches = math.ceil(n_rows / BATCH_SIZE)
        idx = 0
        pbar = tqdm(total=n_rows, desc="Embedding entries")

        for _ in range(total_batches):
            remaining = n_rows - idx
            if remaining <= 0:
                break
            batch_qids = all_qids[idx: idx + min(BATCH_SIZE, remaining)]
            texts = all_texts[idx: idx + min(BATCH_SIZE, remaining)]
            embeddings = embed_batch(texts, tokenizer, model)

            dod_strs = [deaths.get(qid, "") for qid in batch_qids]
            years = []
            for s in dod_strs:
                try:
                    years.append(float(datetime.strptime(s, "%Y-%m-%d").year))
                except Exception:
                    years.append(np.nan)

            end = idx + len(batch_qids)
            emb_ds[idx:end] = embeddings
            qid_ds[idx:end] = batch_qids
            dod_ds[idx:end] = dod_strs
            dod_year_ds[idx:end] = years
            idx = end
            pbar.update(len(batch_qids))
            h5.flush()

        pbar.close()
        print(f"✅ Wrote {idx} entries (with fp16 embeddings) to {out_path}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=(
            "Embed texts from a SQLite table using a custom QID list (one QID per line)"
        )
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--deaths", type=Path, default=DEFAULT_BD, help="Path to dates of death JSON")
    parser.add_argument("--list", dest="list_path", type=Path, default=DEFAULT_LIST,
                        help="Path to text file containing QIDs to embed")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output HDF5 file")
    parser.add_argument(
        "--max-embeddings",
        type=int,
        default=DEFAULT_MAX_EMBEDDINGS,
        help="Stop after embedding this many entries",
    )
    args = parser.parse_args()
    main(args.db, args.deaths, args.list_path, args.out, args.max_embeddings)
