#!/usr/bin/env python3
import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH = BASE / "wikidata_labeled.db"


def decode_text(text: str) -> str:
    r"""Decode escape sequences like \uXXXX into real characters."""
    return text.encode("utf-8").decode("unicode_escape")


def main():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT rowid, value FROM properties WHERE pid='description'")
    rows = cur.fetchall()
    count = 0

    for rowid, value in rows:
        if value is None:
            continue
        decoded = decode_text(value)
        if decoded != value:
            cur.execute(
                "UPDATE properties SET value=? WHERE rowid=?",
                (decoded, rowid),
            )
            count += 1

    conn.commit()
    conn.close()
    print(f"\u2705 Decoded {count} descriptions in {DB_PATH}")


if __name__ == "__main__":
    main()