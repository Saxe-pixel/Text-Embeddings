import bz2
import re
import json
from tqdm import tqdm

# def build_text_representation(qid: str, facts: dict, label_map: dict = None) -> str:
#     """
#     Build a natural-language-style sentence for a Wikidata entity.
#     Tries to follow the style used in the Embedding Project demo.
#     """
#     label = label_map.get(qid, qid) if label_map else qid
#     parts = []

#     for pid, values in facts.items():
#         prop_label = label_map.get(pid, pid) if label_map else pid
#         readable_values = [label_map.get(val, val) if label_map else val for val in values]

#         # Format like "born in Ulm", "educated at Harvard"
#         # Special logic for common PIDs can be added here if desired
#         if len(readable_values) == 1:
#             parts.append(f"{prop_label} {readable_values[0]}")
#         else:
#             joined_vals = ", ".join(readable_values)
#             parts.append(f"{prop_label} {joined_vals}")

#     text = f"{label} is " + ", ".join(parts) + "."
#     return text


def build_text_representation(qid: str, facts: dict, label_map: dict = None) -> str:
    """
    Build a natural-language-style sentence for a Wikidata entity.
    Filters out URLs for cleaner, human-readable output.
    """
    label = label_map.get(qid, qid)
    parts = []

    PID_TEMPLATES = {
        "place of birth": "was born in",
        "place of death": "died in",
        "occupation": "worked as",
        "spouse": "was married to",
        "country of citizenship": "was a citizen of",
        "position held": "held the position of",
        "award received": "received the award",
        "member of": "was a member of",
        "military rank": "held the military rank of",
        "residence": "lived in",
        "native language": "spoke",
        "sex or gender": "was",
        "educated at": "was educated at",
        "religion or worldview": "practiced",
        "sibling": "had siblings including",
        "child": "had children including",
        "father": "had a father named",
        "mother": "had a mother named",
    }

    for pid, values in facts.items():
        prop_label = label_map.get(pid, pid)
        readable_vals = [label_map.get(val, val) for val in values]

        # Filter out raw URLs or commons links
        filtered_vals = [val for val in readable_vals if not val.startswith("http")]
        if not filtered_vals:
            continue

        joined_vals = ", ".join(filtered_vals)
        phrase = PID_TEMPLATES.get(prop_label, f"{prop_label}")
        parts.append(f"{phrase} {joined_vals}")

    return f"{label} {', '.join(parts)}."

def batch_build_texts(human_facts: dict, label_map: dict = None) -> dict:
    """
    Build text representations for a batch of entities.
    Returns a dict: {QID: text_string}
    """
    return {
        qid: build_text_representation(qid, facts, label_map)
        for qid, facts in human_facts.items()
    }

def extract_relevant_ids(human_facts: dict) -> set:
    """
    Extracts all QIDs and PIDs from a human_facts dictionary for targeted label loading.
    """
    ids = set()
    for qid, props in human_facts.items():
        ids.add(qid)
        for pid, values in props.items():
            ids.add(pid)
            ids.update(val for val in values if val.startswith('Q') or val.startswith('P'))
    return ids

def load_labels_for_ids(file_path: str, relevant_ids: set, max_lines=None, save_path: str = None) -> dict:
    """
    Loads English rdfs:label entries for a specified set of Q/P codes.
    Only labels in relevant_ids will be stored in the resulting dictionary.
    """
    label_map = {}
    pattern = re.compile(r'<http://www.wikidata.org/entity/(Q\d+|P\d+)> .*<http://www.w3.org/2000/01/rdf-schema#label> "(.*?)"@en')

    with bz2.open(file_path, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f, desc="🔤 Loading selected labels")):
            match = pattern.match(line)
            if match:
                entity_id, label = match.groups()
                if entity_id in relevant_ids:
                    label_map[entity_id] = label

            if max_lines and i + 1 >= max_lines:
                break

    if save_path:
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(label_map, f, ensure_ascii=False, indent=2)
        print(f"💾 Saved {len(label_map):,} selected labels to {save_path}")

    return label_map

def load_labels_from_cache(cache_path: str) -> dict:
    """
    Load a saved label map from a JSON file.
    """
    with open(cache_path, 'r', encoding='utf-8') as f:
        return json.load(f)
