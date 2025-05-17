#!/usr/bin/env python3
import argparse
import bz2
import json
import re
import os
from pathlib import Path
from tqdm import tqdm

# regex to pull out a subject or object URI like <.../Q42> or <.../P31>
ID_URI_RE = re.compile(r'<http://www\.wikidata\.org/(?:entity|prop/direct)/(Q\d+|P\d+)>')
# regex to detect an English label triple
LABEL_RE = re.compile(
    r'<http://www\.wikidata\.org/entity/(Q\d+|P\d+)>\s+'
    r'<(?:http://www\.w3\.org/2000/01/rdf-schema#label|'
      r'http://www\.w3\.org/2004/02/skos/core#prefLabel|'
      r'http://schema\.org/name)>\s+'
    r'"(.+?)"@en\s*\.\s*$'
)

def load_humans(path):
    return { line.strip() for line in open(path, 'r', encoding='utf-8') if line.strip() }

def collect_ids(dump_path, humans):
    needed = set(humans)
    total = os.path.getsize(dump_path)
    with bz2.open(dump_path, 'rt', encoding='utf-8') as f, \
         tqdm(total=total, unit='B', unit_scale=True, desc="Collecting IDs") as pbar:
        for line in f:
            pbar.update(len(line.encode('utf-8')))
            # split first three tokens
            parts = line.strip().split(' ', 3)
            if len(parts) < 3: 
                continue
            subj_uri, pid_uri, obj = parts[:3]
            m = ID_URI_RE.match(subj_uri)
            if not m:
                continue
            qid = m.group(1)
            if qid not in humans:
                continue
            # subject is human → collect this PID
            pm = ID_URI_RE.match(pid_uri)
            if pm:
                needed.add(pm.group(1))
            # if object is a QID, collect it too
            om = ID_URI_RE.match(obj)
            if om:
                needed.add(om.group(1))
    return needed

def extract_labels(dump_path, needed_ids):
    mapping = {}
    total = os.path.getsize(dump_path)
    with bz2.open(dump_path, 'rt', encoding='utf-8') as f, \
         tqdm(total=total, unit='B', unit_scale=True, desc="Extracting labels") as pbar:
        for line in f:
            pbar.update(len(line.encode('utf-8')))
            m = LABEL_RE.match(line)
            if not m:
                continue
            eid, label = m.groups()
            if eid in needed_ids and eid not in mapping:
                mapping[eid] = label
    return mapping

def main():
    p = argparse.ArgumentParser(
        description="Build JSON label-map for only the IDs touching your human QIDs"
    )
    p.add_argument(
        "--dump",
        type=Path,
        default=Path(__file__).parent.parent / "WikiData.nosync" / "latest-truthy.nt.bz2",
        help="Path to truthy .nt.bz2 dump"
    )
    p.add_argument(
        "--humans",
        type=Path,
        default=Path(__file__).parent.parent / "WikiData.nosync" / "human_qids.txt",
        help="File of human QIDs (one per line)"
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).parent.parent / "WikiData.nosync" / "label_map_full.json",
        help="Output JSON with ID→English label"
    )
    args = p.parse_args()

    print("Loading human QIDs…")
    humans = load_humans(args.humans)
    print(f" → {len(humans):,} humans")

    print("Scanning dump to collect all relevant Q/P IDs…")
    needed_ids = collect_ids(str(args.dump), humans)
    print(f" → {len(needed_ids):,} total IDs to label")

    print("Extracting English labels for those IDs…")
    label_map = extract_labels(str(args.dump), needed_ids)
    print(f" → found {len(label_map):,} labels")

    print("Writing JSON to", args.out)
    with open(args.out, 'w', encoding='utf-8') as f:
        json.dump(label_map, f, ensure_ascii=False, indent=2)

    print("Done.")

if __name__ == "__main__":
    main()
