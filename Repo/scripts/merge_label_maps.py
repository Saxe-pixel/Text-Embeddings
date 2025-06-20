#!/usr/bin/env python3
# Run this after executing find_missing_vals.py to update the label map.
"""Merge label maps after running find_missing_vals.py."""
import json
from pathlib import Path

# Resolve base paths relative to repo root
BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
FULL_MAP = BASE / "label_map_full.json"
FOUND_MAP = BASE / "missing_found.json"
OUT_PATH = BASE / "label_map_complete.json"


def main():
    # load existing full map and the newly found labels
    with open(FULL_MAP, "r", encoding="utf-8") as f:
        full = json.load(f)
    with open(FOUND_MAP, "r", encoding="utf-8") as f:
        found = json.load(f)

    # merge maps giving precedence to newly found labels
    full.update(found)

    # write the combined map back out
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(full, f, ensure_ascii=False, indent=2)
    print(f"✅ Wrote merged label map to {OUT_PATH}")


if __name__ == "__main__":
    main()
