#!/usr/bin/env python3
import sqlite3
import json
import re
import sys
from pathlib import Path

# — adjust these paths to your repo layout —
# Repo/
#   └─ scripts/
#       └─ generate_text.py
#   └─ WikiData.nosync/
BASE        = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
DB_PATH     = BASE / "wikidata.db"
LABELS_JSON = BASE / "label_map_full.json"
# ——————————————————————————————————————————————————————

# load your mapping "Q42"→"Douglas Adams", "P31"→"instance of", etc.
with open(LABELS_JSON, "r", encoding="utf-8") as f:
    LABELS = json.load(f)

# simple English month names
_MONTHS = {
    "01":"Jan","02":"Feb","03":"Mar","04":"Apr","05":"May","06":"Jun",
    "07":"Jul","08":"Aug","09":"Sep","10":"Oct","11":"Nov","12":"Dec"
}

def label_for(id_):
    """Map a Q/P code to its English label, or fall back to the code."""
    return LABELS.get(id_, id_)

def format_date(iso):
    """Turn 'YYYY-MM-DD' into 'YYYY Mon D'."""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", iso)
    if not m:
        return iso
    y,mo,da = m.groups()
    return f"{y} {_MONTHS.get(mo,mo)} {int(da)}"

# PIDs we treat as aliases
ALIAS_PIDS = {"P1449","P1477","P1559"}

# bullet‐line headings (non-past-tense)
ATTRIBUTE_HEADINGS = {
    "instance of":           "instance of",
    "sex or gender":         "sex or gender",
    "occupation":            "occupation",
    "spouse":                "spouse",
    "country of citizenship":"country of citizenship",
    "position held":         "position held",
    "award received":        "award received",
    "member of":             "member of",
    "military rank":         "military rank",
    "residence":             "residence",
    "native language":       "native language",
    "educated at":           "educated at",
    "religion or worldview": "religion or worldview",
    "sibling":               "sibling",
    "child":                 "child",
    "father":                "father",
    "mother":                "mother",
    "date of birth":         "date of birth",
    "date of death":         "date of death",
    "place of birth":        "place of birth",
    "place of death":        "place of death",
}

# regexes
QID_URI_RE   = re.compile(r'^<http://www\.wikidata\.org/entity/(Q\d+)>$')
BARE_QID_RE  = re.compile(r'^(Q\d+)$')
XSD_DT_RE    = re.compile(r'^"([^"]+)"\^\^<http://www\.w3\.org/2001/XMLSchema#dateTime>$')
UN_TYPED_DT  = re.compile(r'^"(\d{4}-\d{2}-\d{2})T')

def fetch_triples(qid):
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("SELECT pid, value FROM properties WHERE qid=?", (qid,))
    rows = cur.fetchall()
    conn.close()
    return rows

def fmt_value(raw):
    """
    1) bare QID → label
    2) <…/QID> → label
    3) typed xsd:dateTime → format_date
    4) untyped ISO datetime → format_date
    5) else strip quotes
    """
    # bare QID?
    if m:=BARE_QID_RE.match(raw):
        return label_for(m.group(1))
    # URI‐wrapped QID?
    if m:=QID_URI_RE.match(raw):
        return label_for(m.group(1))
    # typed dateTime?
    if m:=XSD_DT_RE.match(raw):
        return format_date(m.group(1))
    # untyped ISO datetime?
    if m:=UN_TYPED_DT.match(raw):
        return format_date(m.group(1))
    # fallback
    return raw.strip('"')

def generate_description(qid):
    rows = fetch_triples(qid)

    # gather headline bits
    name    = label_for(qid)
    desc    = None
    birth   = None
    death   = None
    aliases = []

    props = {}  # {prop_label: [values…]}

    for pid_uri, raw in rows:
        code = pid_uri.strip("<>").rsplit("/",1)[-1]
        txt  = fmt_value(raw)

        # description?
        if pid_uri.endswith("/description>"):
            desc = txt; continue
        # lifespan
        if code=="P569": birth = txt; continue
        if code=="P570": death = txt; continue
        # aliases
        if code in ALIAS_PIDS:
            aliases.append(txt); continue

        # everything else…
        label = label_for(code)
        props.setdefault(label, []).append(txt)

    # build first line
    parts = [name]
    if desc:
        parts.append(desc)
    if birth or death:
        span = f"{birth or ''}–{death or ''}"
        parts[-1] += f" ({span})"
    if aliases:
        parts.append("also known as " + ", ".join(aliases))
    headline = "; ".join(parts).strip(" ;") + "."

    # now each attribute on its own line
    lines = [headline, "Attributes include:"]
    for prop, vals in sorted(props.items(), key=lambda x: x[0].lower()):
        key = prop.lower()
        heading = ATTRIBUTE_HEADINGS.get(key, prop)
        lines.append(f"- {heading}: " + ", ".join(vals))

    return "\n".join(lines)

def test_labels():
    for tid in ["Q42","Q14623683","P31","Q5"]:
        print(f"{tid} → {label_for(tid)}")

if __name__=="__main__":
    if len(sys.argv)==2 and sys.argv[1]=="--test-labels":
        test_labels()
        sys.exit(0)

    if len(sys.argv)!=2:
        print("Usage:\n  ./generate_text.py Q42\n  ./generate_text.py --test-labels")
        sys.exit(1)

    print(generate_description(sys.argv[1]))
