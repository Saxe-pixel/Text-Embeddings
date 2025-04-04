import bz2
import re
import json
from tqdm import tqdm


def build_text_representation(qid: str, facts: dict, label_map: dict = None) -> str:
    """
    Convert a single Wikidata entity's fact dictionary to a plain text string.
    If a label_map is provided, replace property/QID codes with readable labels.
    """
    lines = []
    lines.append(f"{label_map.get(qid, qid) if label_map else qid} has the following attributes:")

    for pid, values in facts.items():
        # Map property ID to readable label if available
        prop_label = label_map.get(pid, pid) if label_map else pid

        # Map object QIDs to readable labels if available
        readable_values = [label_map.get(val, val) if label_map else val for val in values]

        # Make it more readable: join multiple values
        val_str = ", ".join(readable_values)
        lines.append(f"- {prop_label}: {val_str}")

    return "\n".join(lines)


def batch_build_texts(human_facts: dict, label_map: dict = None) -> dict:
    """
    Build text representations for a batch of entities.
    Returns a dict: {QID: text_string}
    """
    return {
        qid: build_text_representation(qid, facts, label_map)
        for qid, facts in human_facts.items()
    }


def load_labels(file_path: str, max_lines=None, save_path: str = None, save_every=50_000_000) -> dict:
    """
    Loads English rdfs:label entries from the .nt.bz2 file.
    Returns a dictionary mapping Q/P codes to human-readable labels.
    Periodically saves the label map to avoid memory overflow.
    """
    label_map = {}
    pattern = re.compile(r'<http://www.wikidata.org/entity/(Q\d+|P\d+)> .*<http://www.w3.org/2000/01/rdf-schema#label> "(.*?)"@en')

    with bz2.open(file_path, 'rt', encoding='utf-8') as f:
        for i, line in enumerate(tqdm(f, desc="🔤 Loading labels")):
            match = pattern.match(line)
            if match:
                entity_id, label = match.groups()
                label_map[entity_id] = label

            if save_path and (i + 1) % save_every == 0:
                with open(save_path, 'w', encoding='utf-8') as f_out:
                    json.dump(label_map, f_out, ensure_ascii=False, indent=2)
                print(f"💾 [Checkpoint] Saved {len(label_map):,} labels to {save_path} at line {i + 1:,}")

            if max_lines and i + 1 >= max_lines:
                break

    if save_path:
        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(label_map, f, ensure_ascii=False, indent=2)
        print(f"💾 [Final] Saved {len(label_map):,} labels to {save_path}")

    return label_map


def load_labels_from_cache(cache_path: str) -> dict:
    """
    Load a saved label map from a JSON file.
    """
    with open(cache_path, 'r', encoding='utf-8') as f:
        return json.load(f)
