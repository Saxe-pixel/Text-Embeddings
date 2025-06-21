#!/usr/bin/env python3
"""Generate simple text descriptions from ``wikidata_labeled.db``.

For each QID listed in a text file this script builds a short text of the
form::

    qid | qid_label, description
    "Attributes include:"
    property_label: value_label
    ...

The resulting texts are stored in a SQLite table ``texts`` with columns
``qid`` and ``text``. The QID column is indexed so lookups are fast.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

# scripts/ -> Repo/ -> Text-Embeddings/ -> WikiData.nosync/
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DEFAULT_DB = BASE / "wikidata_labeled_wo.db"
DEFAULT_QIDS = BASE / "qids-test.txt"
DEFAULT_OUT = BASE / "qid_texts-test.db"
TABLE_NAME = "texts"


def load_qids(path: Path) -> list[str]:
    """Return all QIDs from the given text file."""
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]


def has_column(cur: sqlite3.Cursor, table: str, column: str) -> bool:
    cur.execute(f"PRAGMA table_info({table})")
    return any(row[1] == column for row in cur.fetchall())


def fetch_rows(
    cur: sqlite3.Cursor,
    table: str,
    qid: str,
    use_value_label: bool,
    use_qid_label: bool,
) -> list[tuple[str | None, str | None, str | None, str | None]]:
    """Fetch all rows for a QID returning label and raw value."""

    if use_qid_label:
        qid_col = "qid_label"
    else:
        qid_col = "? AS qid_label"

    if use_value_label:
        val_label_col = "value_label"
    else:
        val_label_col = "NULL AS value_label"

    cur.execute(
        f"SELECT {qid_col}, property_label, {val_label_col}, value FROM {table} WHERE qid=?",
        (qid,) if use_qid_label else (qid, qid),
    )
    return cur.fetchall()


def build_text(
    qid: str,
    rows: list[tuple[str | None, str | None, str | None, str | None]],
) -> str:
    """Turn DB rows into a short descriptive text."""

    qlabel: str | None = None
    description = ""
    attributes: list[str] = []

    for qid_label, prop_label, val_label, raw_value in rows:
        if qid_label and qlabel is None:
            # Some databases store "Q42|Douglas Adams" in the qid_label
            # column.  In that case we only want the human-readable label.
            qlabel = qid_label.split("|", 1)[-1]
        value = val_label if val_label else raw_value
        if prop_label and prop_label.lower() == "description":
            description = value or ""
        else:
            if prop_label and value:
                attributes.append(f"{prop_label}: {value}")

    if qlabel is None:
        qlabel = qid
    headline = f"{qid} | {qlabel}, {description}".strip().strip(",")
    lines = [headline, "Attributes include:"]
    lines.extend(attributes)
    return "\n".join(lines)


def main(db_path: Path, qids_file: Path, out_db: Path) -> None:
    qids = load_qids(qids_file)

    src_conn = sqlite3.connect(db_path)
    src_cur = src_conn.cursor()

    # Determine table name and available columns
    table = "properties_labeled"
    src_cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    )
    if not src_cur.fetchone():
        table = "properties"

    use_qid_label = has_column(src_cur, table, "qid_label")
    use_value_label = has_column(src_cur, table, "value_label")

    out_conn = sqlite3.connect(out_db)
    out_cur = out_conn.cursor()
    out_cur.execute(
        f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (qid TEXT PRIMARY KEY, text TEXT)"
    )
    out_cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_qid ON {TABLE_NAME}(qid)"
    )

    for qid in qids:
        rows = fetch_rows(src_cur, table, qid, use_value_label, use_qid_label)
        if not rows:
            continue
        text = build_text(qid, rows)
        out_cur.execute(
            f"INSERT OR REPLACE INTO {TABLE_NAME} VALUES (?, ?)",
            (qid, text),
        )

    out_conn.commit()
    out_conn.close()
    src_conn.close()
    print(f"\u2705 Wrote {len(qids)} texts to {out_db}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate short texts for QIDs from wikidata_labeled.db",
    )
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--qids", type=Path, default=DEFAULT_QIDS, help="Text file with QIDs")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Output SQLite database")
    args = parser.parse_args()
    main(args.db, args.qids, args.out)
