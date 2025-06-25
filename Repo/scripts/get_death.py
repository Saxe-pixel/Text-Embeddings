#!/usr/bin/env python3
"""Extract birth date values from ``wikidata_labeled_wo.db``.

The script queries the ``properties_labeled`` table for all rows
where the property id (``pid``) equals ``P570``.  For each matching row
the ``qid`` and raw ``value`` are collected and written to
``death_dates.json`` as a simple ``{qid: value}`` mapping.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "wikidata_labeled_wo.db"
DEFAULT_OUT = BASE / "death_dates.json"

def fetch_birthdays(cur: sqlite3.Cursor) -> dict[str, str]:
    """Return a mapping of QID to death date (value column)."""
    cur.execute(
        "SELECT qid, value FROM properties_labeled WHERE pid='P570'"  # P570 is the property for death date
    )
    return {
        qid: value
        for qid, value in cur.fetchall()
        if qid is not None and value is not None
    }


def main(db_path: Path, out_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    data = fetch_birthdays(cur)
    conn.close()

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"\u2705 Wrote {len(data)} dates of death to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract birthday values from wikidata_labeled_wo.db")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output JSON file")
    args = parser.parse_args()
    main(args.db, args.out)