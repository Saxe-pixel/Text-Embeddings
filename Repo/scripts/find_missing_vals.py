#!/usr/bin/env python3
import bz2
import json
import os
import re
from pathlib import Path
from tqdm import tqdm

BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
MISSING_FILE = BASE / "missing_label.txt"
DUMP_PATH = BASE / "latest-truthy.nt.bz2"
OUT_JSON = BASE / "missing_found.json"

LABEL_EN_RE = re.compile(
    r'<http://www\.wikidata\.org/entity/(Q\d+|P\d+)>\s+'
    r'<(?:http://www\.w3\.org/2000/01/rdf-schema#label|'
    r'http://www\.w3\.org/2004/02/skos/core#prefLabel|'
    r'http://schema\.org/name)>\s+'
    r'"(.+?)"@en\s*\.\s*$'
)

LABEL_MUL_RE = re.compile(
    r'<http://www\.wikidata\.org/entity/(Q\d+|P\d+)>\s+'
    r'<(?:http://www\.w3\.org/2000/01/rdf-schema#label|'
    r'http://www\.w3\.org/2004/02/skos/core#prefLabel|'
    r'http://schema\.org/name)>\s+'
    r'"(.+?)"@mul\s*\.\s*$'
)

def load_missing():
    with open(MISSING_FILE, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def main():
    missing_ids = load_missing()
    pending = set(missing_ids)
    found = {}
    total = os.path.getsize(DUMP_PATH)
    with bz2.open(DUMP_PATH, 'rt', encoding='utf-8') as f, \
         tqdm(total=total, unit='B', unit_scale=True, desc='Searching labels') as pb:
        for line in f:
            pb.update(len(line.encode('utf-8')))
            m = LABEL_EN_RE.match(line)
            if m:
                eid, label = m.groups()
                if eid in pending:
                    found[eid] = label
                    pending.remove(eid)
                    if not pending:
                        break
                continue
            m = LABEL_MUL_RE.match(line)
            if m:
                eid, label = m.groups()
                if eid in pending and eid not in found:
                    found[eid] = label
                    pending.remove(eid)
                    if not pending:
                        break
    with open(OUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(found, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()
