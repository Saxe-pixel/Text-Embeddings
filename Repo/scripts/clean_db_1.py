#!/usr/bin/env python3
"""Remove unwanted label rows from wikidata_labeled.db."""
import sqlite3
from pathlib import Path
import argparse

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH = BASE / "wikidata_labeled.db"

TARGET_PROPERTIES = {
    "core#altLabel",
    "core#prefLabel",
    "rdf-schema#label",
    "birth name",
    "name",
    "dateModified",
}


def clean_database(db_path: Path = DB_PATH) -> None:
    """Delete rows with unwanted property labels."""
    conn = sqlite3.connect(db_path)
    placeholders = ",".join("?" for _ in TARGET_PROPERTIES)
    cur = conn.execute(
        f"DELETE FROM properties_labeled WHERE property_label IN ({placeholders})",
        tuple(TARGET_PROPERTIES),
    )
    conn.commit()
    print(f"\U0001F5D1\uFE0F Removed {cur.rowcount} rows from {db_path}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Remove altLabel/prefLabel/name rows from wikidata_labeled.db"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help="Path to SQLite database",
    )
    args = parser.parse_args()
    clean_database(args.db)
