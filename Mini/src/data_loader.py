


def load_wikidata5m_text(filepath, max_lines=None):
    """
    Load wikidata5m_text.txt and return (entity_id, text) pairs.
    Assumes each line has either:
      - QID<TAB>text  (2 columns)
      - or QID<TAB>PID<TAB>QID/text<TAB>text (4 columns)
    """
    data = []
    with open(filepath, 'r', encoding='utf-8') as f:
        for i, line in enumerate(f):
            parts = line.strip().split('\t')
            if len(parts) == 2:
                entity_id, text = parts
            elif len(parts) == 4:
                entity_id, _, _, text = parts
            else:
                continue  # malformed line

            data.append((entity_id, text))

            if max_lines and i + 1 >= max_lines:
                break

    return data