#!/usr/bin/env python3
import sqlite3
import json
import re
from pathlib import Path

BASE          = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_IN         = BASE / "wikidata.db"
DB_OUT        = BASE / "wikidata_labeled.db"
LABELS_JSON   = BASE / "label_map_full.json"
MISSING_FILE  = BASE / "missing_label.txt"

DATE_RE = re.compile(r'^"?(\d{4}-\d{2}-\d{2})')

def load_labels():
    with open(LABELS_JSON, encoding="utf-8") as f:
        return json.load(f)

def strip_date(value):
    m = DATE_RE.match(value)
    if m:
        return m.group(1)
    return value

def clean_value(value):
    if value.startswith("<http://www.wikidata.org/entity/"):
        return value.rsplit("/", 1)[-1].strip(">")
    return strip_date(value.strip('"'))

def main():
    labels = load_labels()
    missing = set()

    src = sqlite3.connect(DB_IN)
    dst = sqlite3.connect(DB_OUT)
    dst.execute("""
        CREATE TABLE IF NOT EXISTS properties_labeled (
            qid TEXT,
            qid_label TEXT,
            pid TEXT,
            property_label TEXT,
            value TEXT,
            value_label TEXT
        )
    """)

    for qid, pid, value in src.execute("SELECT qid, pid, value FROM properties"):
        qlabel = labels.get(qid)
        if qlabel is None:
            missing.add(qid)
        plabel = labels.get(pid)
        if plabel is None:
            missing.add(pid)

        clean_val = clean_value(value)
        vlabel = None
        if clean_val.startswith(("Q", "P")):
            vlabel = labels.get(clean_val)
            if vlabel is None:
                missing.add(clean_val)

        dst.execute(
            "INSERT INTO properties_labeled VALUES (?, ?, ?, ?, ?, ?)",
            (qid, qlabel, pid, plabel, clean_val, vlabel)
        )

    dst.commit()
    src.close()
    dst.close()

    with open(MISSING_FILE, "w", encoding="utf-8") as f:
        for ident in sorted(missing):
            f.write(f"{ident}\n")

if __name__ == "__main__":
    main()