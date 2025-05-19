#!/usr/bin/env python3
import sqlite3
import json
import re
from pathlib import Path

# — adjust these paths to your repo layout —
# scripts/ → Repo/ → Text-Embeddings/ → WikiData.nosync/
BASE        = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH     = BASE / "wikidata.db"
LABELS_JSON = BASE / "label_map_full.json"
# ——————————————————————————————————————————————————————

# load your JSON mapping "Q42"→"Douglas Adams", "P31"→"instance of", etc.
with open(LABELS_JSON, "r", encoding="utf-8") as f:
    LABELS = json.load(f)

# custom phrasing per property
PID_TEMPLATES = {
    "place of birth":           "was born in",
    "place of death":           "died in",
    "occupation":               "worked as",
    "spouse":                   "was married to",
    "country of citizenship":   "was a citizen of",
    "position held":            "held the position of",
    "award received":           "received the award",
    "member of":                "was a member of",
    "military rank":            "held the military rank of",
    "residence":                "lived in",
    "native language":          "spoke",
    "sex or gender":            "was",
    "educated at":              "was educated at",
    "religion or worldview":    "practiced",
    "sibling":                  "had siblings including",
    "child":                    "had children including",
    "father":                   "had a father named",
    "mother":                   "had a mother named",
}

# helper: get human-readable label for any ID (Q or P)
def label_for(id_):
    return LABELS.get(id_, id_)

# pull all triples for one QID
def fetch_triples(qid):
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("""
        SELECT pid, value
          FROM properties
         WHERE qid = ?
    """, (qid,))
    rows = cur.fetchall()
    conn.close()
    return rows

# format a single value: if it's a URI <…/Q123> return its label, else a literal as-is
QID_RE = re.compile(r'^<http://www\.wikidata\.org/entity/(Q\d+)>$')
def fmt_value(raw):
    m = QID_RE.match(raw)
    if m:
        return label_for(m.group(1))
    # otherwise it's a literal string like Douglas Adams or "1952–2001"
    return raw.strip('"')

def generate_description(qid):
    subject_label = label_for(qid)
    triples       = fetch_triples(qid)
    phrases       = []

    for pid_uri, raw_val in triples:
        # extract just the PID code and find its English label
        pid_code     = pid_uri.rstrip(">").rsplit("/", 1)[-1]
        prop_label   = label_for(pid_code)
        val_text     = fmt_value(raw_val)

        # choose template if it exists (case‐insensitive match)
        tpl = PID_TEMPLATES.get(prop_label.lower())
        if tpl:
            phrases.append(f"{tpl} {val_text}")
        else:
            phrases.append(f"{prop_label} {val_text}")

    # join with commas; you can tweak punctuation here as needed
    return f"{subject_label}, " + ", ".join(phrases)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: generate_text.py Q42")
        sys.exit(1)
    q = sys.argv[1]
    print(generate_description(q))
