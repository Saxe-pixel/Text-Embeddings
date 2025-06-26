#!/usr/bin/env python3
"""Embed texts and store them in an HDF5 file, with MPS/CPU auto-selection,
stable ordering, progress reporting, fp16 embeddings, and cache clearing to
reduce memory. Supports stopping after a specified number of embeddings."""

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
BATCH_SIZE = 4    # keep small to limit per-batch memory

DEFAULT_MAX_EMBEDDINGS = 10000 # limit to avoid infinite runs (if set to None, no limit)

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "qid_texts_wo_m_clean.db"
DEFAULT_BD = BASE / "death_dates_clean.json"
DEFAULT_OUT = BASE / "people_embeddings_death.h5"

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


def main(
    db_path: Path,
    death_path: Path,
    out_path: Path,
    max_embeddings: int | None = DEFAULT_MAX_EMBEDDINGS,
) -> None:
    """Embed texts and store them with associated dates of death.

    ``max_embeddings`` sets how many entries to process before stopping. It
    defaults to :data:`DEFAULT_MAX_EMBEDDINGS` (200k) so very large databases
    don't run indefinitely.
    """
    # connect to SQLite
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    n_rows_total = cur.fetchone()[0]

    # limit how many embeddings to compute if a maximum is provided
    n_rows = min(n_rows_total, max_embeddings) if max_embeddings else n_rows_total
    cur.execute(f"SELECT MAX(LENGTH(qid)) FROM {TABLE_NAME}")
    max_qid_len = cur.fetchone()[0]

    # load dates of death JSON
    with open(death_path, "r", encoding="utf-8") as f:
        deaths = json.load(f)

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

        # load tokenizer & model onto DEVICE
        tokenizer, model = load_model(DEVICE)

        # stable ordering by qid (ensures aligned datasets)
        cur.execute(f"SELECT qid, text FROM {TABLE_NAME} ORDER BY qid")

        total_batches = math.ceil(n_rows / BATCH_SIZE)
        idx = 0
        pbar = tqdm(total=n_rows, desc="Embedding entries")

        for _ in range(total_batches):
            remaining = n_rows - idx
            if remaining <= 0:
                break
            rows = cur.fetchmany(min(BATCH_SIZE, remaining))
            if not rows:
                break
            qids = [r[0] for r in rows]
            texts = [r[1] for r in rows]
            embeddings = embed_batch(texts, tokenizer, model)

            # prepare DOD strings and numeric years
            dod_strs = [deaths.get(qid, "") for qid in qids]
            years = []
            for s in dod_strs:
                try:
                    years.append(float(datetime.strptime(s, "%Y-%m-%d").year))
                except Exception:
                    years.append(np.nan)

            end = idx + len(rows)
            emb_ds[idx:end] = embeddings
            qid_ds[idx:end] = qids
            dod_ds[idx:end] = dod_strs
            dod_year_ds[idx:end] = years
            idx = end
            pbar.update(len(rows))

            # flush to disk and free any HDF5 cache
            h5.flush()

        pbar.close()

        print(f"\u2705 Wrote {idx} entries (with fp16 embeddings) to {out_path}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Embed texts from a SQLite table")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--deaths", type=Path, default=DEFAULT_BD, help="Path to dates of death JSON")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output HDF5 file")
    parser.add_argument(
        "--max-embeddings",
        type=int,
        default=DEFAULT_MAX_EMBEDDINGS,
        help="Stop after embedding this many entries (default: 200000)",
    )
    args = parser.parse_args()
    main(args.db, args.deaths, args.out, args.max_embeddings)