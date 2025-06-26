#!/usr/bin/env python3
"""Embed texts and store them in an HDF5 file, now forcing CPU by default
to avoid MPS unified‐memory bloat."""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import gc
from pathlib import Path
from datetime import datetime

import h5py
import numpy as np
import torch
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel

MODEL_NAME    = "jinaai/jina-embeddings-v3"
TABLE_NAME    = "texts"
EMB_DIM       = 1024
BATCH_SIZE    = 32       # smallest possible to test stability
DEFAULT_MAX   = 20000

BASE          = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB    = BASE / "qid_texts_wo_clean.db"
DEFAULT_DEATH = BASE / "death_dates_clean.json"
DEFAULT_OUT   = BASE / "people_embeddings_death.h5"


def parse_args():
    p = argparse.ArgumentParser(description="Embed texts from a SQLite table")
    p.add_argument("--db",      type=Path, default=DEFAULT_DB,    help="SQLite DB path")
    p.add_argument("--deaths",  type=Path, default=DEFAULT_DEATH, help="JSON of death dates")
    p.add_argument("--out",     type=Path, default=DEFAULT_OUT,   help="Output HDF5 file")
    p.add_argument("--max-embeddings", type=int, default=DEFAULT_MAX,
                   help="Stop after embedding this many entries")
    p.add_argument("--start",   type=int, default=0,
                   help="Skip this many rows first (for chunked runs)")
    p.add_argument("--device",  choices=("mps", "cpu"), default="cpu",
                   help="Force device (default: CPU to avoid high MPS footprint)")
    return p.parse_args()


def load_model(device: torch.device):
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, trust_remote_code=True)
    model     = AutoModel.from_pretrained(MODEL_NAME,    trust_remote_code=True)
    model.to(device)
    try:
        model.half()
    except Exception:
        pass
    model.eval()
    return tokenizer, model


def embed_batch(texts: list[str], tokenizer, model) -> np.ndarray:
    encoded = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")
    encoded = {k: v.to(model.device) for k, v in encoded.items()}

    with torch.inference_mode():
        output = model(**encoded)

    tokens = output.last_hidden_state
    mask   = encoded["attention_mask"].unsqueeze(-1).expand(tokens.size()).float()
    summed = torch.sum(tokens * mask, dim=1)
    counts = torch.clamp(mask.sum(dim=1), min=1e-9)
    emb    = (summed / counts).cpu().numpy().astype(np.float16)

    del output, tokens, mask, summed, counts, encoded
    gc.collect()
    return emb


def main():
    args   = parse_args()
    DEVICE = torch.device(args.device)
    print(f"⚙️  Running on {DEVICE}")

    conn   = sqlite3.connect(args.db)
    cur    = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total = cur.fetchone()[0]

    cur.execute(f"SELECT MAX(LENGTH(qid)) FROM {TABLE_NAME}")
    max_qid_len = cur.fetchone()[0]

    with open(args.deaths, "r", encoding="utf-8") as f:
        deaths = json.load(f)

    max_run = min(total - args.start, args.max_embeddings)

    with h5py.File(args.out, "w") as h5:
        emb_ds = h5.create_dataset("embeddings", shape=(max_run, EMB_DIM), dtype="f2",
                                   chunks=(1, EMB_DIM), compression="lzf")
        qid_ds = h5.create_dataset("qids",       shape=(max_run,),   dtype=h5py.string_dtype("ascii", max_qid_len))
        dod_ds = h5.create_dataset("dod",        shape=(max_run,),   dtype=h5py.string_dtype("ascii", 10))
        yr_ds  = h5.create_dataset("dod_year",   shape=(max_run,),   dtype="f4")

        tokenizer, model = load_model(DEVICE)
        cur.execute(f"SELECT qid, text FROM {TABLE_NAME} ORDER BY qid")
        if args.start:
            cur.fetchmany(args.start)

        pbar = tqdm(total=max_run, desc="Embedding entries")
        idx  = 0
        batches = math.ceil(max_run / BATCH_SIZE)

        for _ in range(batches):
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            qids  = [r[0] for r in rows]
            texts = [r[1] for r in rows]
            embs  = embed_batch(texts, tokenizer, model)

            dods = [deaths.get(q, "") for q in qids]
            yrs  = []
            for s in dods:
                try:
                    yrs.append(float(datetime.strptime(s, "%Y-%m-%d").year))
                except:
                    yrs.append(np.nan)

            end = idx + len(rows)
            emb_ds[idx:end] = embs
            qid_ds[idx:end] = qids
            dod_ds[idx:end] = dods
            yr_ds[idx:end]  = yrs

            idx += len(rows)
            pbar.update(len(rows))

            h5.flush()
            del qids, texts, embs, dods, yrs
            gc.collect()

        pbar.close()
        print(f"✅ Wrote {idx} entries to {args.out}")

    conn.close()


if __name__ == "__main__":
    main()
