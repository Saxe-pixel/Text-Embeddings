#!/usr/bin/env python3
"""Extract QIDs with a specific property from ``wikidata_labeled.db``.

By default this searches for property ``P569`` (date of birth) and
writes the QIDs to ``qid_with_bd.txt``.
"""
import sqlite3
from pathlib import Path
import argparse

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "wikidata_labeled.db"
DEFAULT_OUT = Path("qid_with_bd.txt")


def extract_qids(db_path: Path, pid: str) -> list[str]:
    """Return all unique QIDs that have the given property ID."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        "SELECT DISTINCT qid FROM properties_labeled WHERE pid = ?",
        (pid,),
    )
    rows = [row[0] for row in cur.fetchall()]
    conn.close()
    return rows


def main(db_path: Path, pid: str, out_path: Path) -> None:
    qids = extract_qids(db_path, pid)
    with open(out_path, "w", encoding="utf-8") as f:
        for qid in qids:
            f.write(f"{qid}\n")
    print(f"\u2705 Wrote {len(qids)} QIDs with {pid} to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract QIDs with a given PID from wikidata_labeled.db",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--pid", default="P569", help="Property ID to search for")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output text file")
    args = parser.parse_args()
    main(args.db, args.pid, args.out)
