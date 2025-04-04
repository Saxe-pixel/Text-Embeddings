


import bz2
import re
from typing import Iterator, Tuple

HUMAN_QID = "Q5"  # Wikidata QID for 'human'
INSTANCE_OF_PID = "P31"

# Regex to extract triples from N-Triples lines
TRIPLE_PATTERN = re.compile(r'<([^>]+)>\s+<([^>]+)>\s+<([^>]+)>\s*\.\s*')

def stream_triples(file_path: str) -> Iterator[Tuple[str, str, str]]:
    """
    Stream and decompress .bz2 file line by line, yielding subject, predicate, object triples.
    Only yields valid <s> <p> <o> . formatted lines.
    """
    with bz2.open(file_path, mode='rt', encoding='utf-8') as f:
        for line in f:
            match = TRIPLE_PATTERN.match(line)
            if match:
                subj, pred, obj = match.groups()
                yield subj, pred, obj

def is_human_instance(triple: Tuple[str, str, str]) -> bool:
    """Check if the triple is of the form: ?s wdt:P31 wd:Q5"""
    subj, pred, obj = triple
    return pred.endswith(f"/prop/direct/{INSTANCE_OF_PID}") and obj.endswith(f"/entity/{HUMAN_QID}")

def get_qid(uri: str) -> str:
    """Extract QID from full URI."""
    return uri.split('/')[-1]

def extract_human_qids(file_path: str, max_count: int = None) -> set:
    """
    Extract all QIDs that are instance of human (P31=Q5).
    """
    human_qids = set()
    for i, (subj, pred, obj) in enumerate(stream_triples(file_path)):
        if is_human_instance((subj, pred, obj)):
            human_qids.add(get_qid(subj))
        if max_count and i >= max_count:
            break
    return human_qids
