#!/usr/bin/env python3
import sqlite3
import json
import re
from pathlib import Path

# ─────── CONFIGURATION ───────
# Toggle each filter by setting to True (enabled) or False (disabled)
FILTER_UNWANTED_PIDS    = True   # 1. Skip core#altLabel, core#prefLabel, etc.
FILTER_MISSING_QID      = True   # 2. Skip rows where qid_label is missing
FILTER_PID_P569         = False  # 3. Skip rows where pid == "P569"
FILTER_PID_P570         = False  # 4. Skip rows where pid == "P570"
# ─────────────────────────────

BASE          = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_IN         = BASE / "wikidata.db"
DB_OUT        = BASE / "wikidata_labeled_wo.db"
LABELS_JSON   = BASE / "label_map_complete_decoded.json"
MISSING_FILE  = BASE / "missing_label.txt"

# PIDs to exclude entirely when FILTER_UNWANTED_PIDS is True
UNWANTED_PIDS = {
    "core#altLabel",
    "core#prefLabel",
    "rdf-schema#label",
    "birth name",
    "name",
    "dateModified",
}

DATE_RE = re.compile(r'^"?(\d{4}-\d{2}-\d{2})')
ID_RE   = re.compile(r'^[PQ]\d+$')

def load_labels():
    with open(LABELS_JSON, encoding="utf-8") as f:
        return json.load(f)

def strip_date(value):
    m = DATE_RE.match(value)
    return m.group(1) if m else value

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
            qid            TEXT,
            qid_label      TEXT,
            pid            TEXT,
            property_label TEXT,
            value          TEXT,
            value_label    TEXT
        )
    """)
    dst.execute('CREATE INDEX IF NOT EXISTS idx_pl_qid ON properties_labeled(qid);')
    dst.execute('CREATE INDEX IF NOT EXISTS idx_pl_pid ON properties_labeled(pid);')

    for qid, pid, value in src.execute("SELECT qid, pid, value FROM properties"):
        # 1. Unwanted-PIDs filter
        if FILTER_UNWANTED_PIDS and pid in UNWANTED_PIDS:
            continue

        # 2. Skip P569?
        if FILTER_PID_P569 and pid == "P569":
            continue

        # 3. Skip P570?
        if FILTER_PID_P570 and pid == "P570":
            continue

        # Look up qid_label (and optionally skip if missing)
        qlabel = labels.get(qid)
        if qlabel is None:
            missing.add(qid)
            if FILTER_MISSING_QID:
                continue

        # Look up property_label (always track missing)
        plabel = labels.get(pid)
        if plabel is None:
            missing.add(pid)

        # Clean & label the value
        clean_val = clean_value(value)
        vlabel = None
        if ID_RE.match(clean_val):
            vlabel = labels.get(clean_val)
            if vlabel is None:
                missing.add(clean_val)

        # Insert!
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
