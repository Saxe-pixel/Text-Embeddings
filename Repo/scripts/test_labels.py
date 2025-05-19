#!/usr/bin/env python3
import json
from pathlib import Path

BASE      = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
JSON_PATH = BASE / "label_map_full.json"

with open(JSON_PATH, encoding="utf-8") as f:
    LABELS = json.load(f)

for test in ["Q42","Q14623683","P31","Q5"]:
    print(f"{test:12s}→", LABELS.get(test, "<missing>"))
