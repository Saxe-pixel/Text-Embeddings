#!/usr/bin/env python3
import json
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent.parent / "WikiData.nosync"
IN_FILE = BASE / "label_map_complete.json"
OUT_FILE = BASE / "label_map_complete_decoded.json"


def main():
    with open(IN_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Decoded labels written to {OUT_FILE}")


if __name__ == "__main__":
    main()