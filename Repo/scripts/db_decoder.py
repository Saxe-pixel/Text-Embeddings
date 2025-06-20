#!/usr/bin/env python3
"""Decode escape sequences for selected properties in wikidata_labeled.db."""
import sqlite3
from pathlib import Path
import argparse

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH = BASE / "wikidata_labeled.db"

# Only these properties contain escaped UTF-8 text
TARGET_PROPERTIES = {
    "core#altLabel",
    "core#prefLabel",
    "rdf-schema#label",
    "birth name",
    "description",
    "name",
}


def decode_text(text: str) -> str:
    """Recursively decode unicode escape sequences."""
    prev = text
    while True:
        try:
            new = prev.encode("latin1").decode("unicode_escape")
        except UnicodeDecodeError:
            break
        if new == prev:
            break
        prev = new
    return prev


def main(db_path: Path = DB_PATH) -> None:
    """Decode text columns for the selected properties."""
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    placeholders = ",".join("?" for _ in TARGET_PROPERTIES)
    cur.execute(
        f"SELECT rowid, qid_label, property_label, value, value_label FROM properties_labeled WHERE property_label IN ({placeholders})",
        tuple(TARGET_PROPERTIES),
    )
    rows = cur.fetchall()
    count = 0

    for row in rows:
        rowid, qid_label, prop_label, value, value_label = row

        dqid = decode_text(qid_label) if qid_label is not None else None
        dprop = decode_text(prop_label) if prop_label is not None else None
        dval = decode_text(value) if value is not None else None
        dval_label = decode_text(value_label) if value_label is not None else None

        if [dqid, dprop, dval, dval_label] != [qid_label, prop_label, value, value_label]:
            cur.execute(
                "UPDATE properties_labeled SET qid_label=?, property_label=?, value=?, value_label=? WHERE rowid=?",
                (dqid, dprop, dval, dval_label, rowid),
            )
            count += 1

    conn.commit()
    conn.close()
    print(f"\u2705 Decoded {count} rows in {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Decode escape sequences in wikidata_labeled.db")
    parser.add_argument("--db", type=Path, default=DB_PATH, help="Path to SQLite database")
    args = parser.parse_args()
    main(args.db)