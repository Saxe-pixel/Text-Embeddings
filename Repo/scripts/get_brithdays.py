#!/usr/bin/env python3
"""Extract birth dates for QIDs listed in ``qid_with_bd.txt``.

This script looks up QIDs in ``wikidata_labeled_wo.db`` and
retrieves the raw value where ``pid`` equals ``P569``.
The results are written as a JSON mapping ``{qid: value}``.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "wikidata_labeled_wo.db"
DEFAULT_QIDS = BASE / "qid_with_bd.txt"
DEFAULT_OUT = BASE / "birthdays.json"


def load_qids(path: Path) -> list[str]:
    """Return QIDs from the given text file."""
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def fetch_birthdays(
    cur: sqlite3.Cursor, qids: list[str], batch_size: int = 900
) -> dict[str, str]:
    """Return a mapping of QID to birth date (value column)."""
    result: dict[str, str] = {}
    if not qids:
        return result

    for i in range(0, len(qids), batch_size):
        chunk = qids[i : i + batch_size]
        placeholders = ",".join("?" for _ in chunk)
        cur.execute(
            f"SELECT qid, value FROM properties_labeled WHERE pid='P569' AND qid IN ({placeholders})",
            chunk,
        )
        result.update(
            {qid: value for qid, value in cur.fetchall() if value is not None}
        )

    return result


def main(db_path: Path, qids_path: Path, out_path: Path) -> None:
    qids = load_qids(qids_path)
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    data = fetch_birthdays(cur, qids)
    conn.close()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\u2705 Wrote {len(data)} birthdays to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract birthday values for QIDs")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--qids", type=Path, default=DEFAULT_QIDS, help="Text file with QIDs")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSON file")
    args = parser.parse_args()
    main(args.db, args.qids, args.out)