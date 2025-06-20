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
    """Decode escape sequences across all text columns."""

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute(
        "SELECT rowid, qid_label, property_label, value, value_label FROM properties_labeled"
    )
    rows = cur.fetchall()
    count = 0

    for row in rows:
        rowid, qid_label, prop_label, value, value_label = row

        decoded_qid_label = decode_text(qid_label) if qid_label is not None else None
        decoded_prop_label = decode_text(prop_label) if prop_label is not None else None
        decoded_value = decode_text(value) if value is not None else None
        decoded_value_label = decode_text(value_label) if value_label is not None else None

        if [decoded_qid_label, decoded_prop_label, decoded_value, decoded_value_label] != [
            qid_label,
            prop_label,
            value,
            value_label,
        ]:
            cur.execute(
                "UPDATE properties_labeled SET qid_label=?, property_label=?, value=?, value_label=? WHERE rowid=?",
                (
                    decoded_qid_label,
                    decoded_prop_label,
                    decoded_value,
                    decoded_value_label,
                    rowid,
                ),
            )
            count += 1

    conn.commit()
    conn.close()
    print(f"\u2705 Decoded {count} rows in {db_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Decode escape sequences across the database"
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=DB_PATH,
        help="Path to SQLite database",
    )
    args = parser.parse_args()
    main(args.db)
