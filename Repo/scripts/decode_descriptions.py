#!/usr/bin/env python3
import sqlite3
from pathlib import Path
import argparse

BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH = BASE / "wikidata_labeled.db"


def decode_text(text: str) -> str:
    r"""Decode escape sequences like \uXXXX into real characters."""

    try:
        return text.encode("utf-8").decode("unicode_escape")
    except UnicodeDecodeError:
        # Fall back to the original string if decoding fails
        return text



def main(db_path: Path = DB_PATH):
    """Decode escape sequences in the description property."""

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("SELECT rowid, value FROM properties_labeled WHERE pid='description'")
    rows = cur.fetchall()
    count = 0

    for rowid, value in rows:
        if value is None:
            continue
        decoded = decode_text(value)
        if decoded != value:
            cur.execute(
                "UPDATE properties_labeled SET value=? WHERE rowid=?",
                (decoded, rowid),
            )
            count += 1

    conn.commit()
    conn.close()

    print(f"\u2705 Decoded {count} descriptions in {db_path}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Decode description texts")
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help="Path to SQLite database",
    )
    args = parser.parse_args()
    main(args.db)