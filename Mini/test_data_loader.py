


from src.data_loader import load_wikidata5m_text

# Point to your file
file_path = "/Users/Saxe/Desktop/GitHub/Data.nosync/wikidata5m_text.txt.nosync.txt"

# from pathlib import Path

# file_path = Path(__file__).parent / "data" / "wikidata5m_text.txt.nosync"


# Load the first 5 lines for testing
data = load_wikidata5m_text(file_path, max_lines=5)

for entity_id, text in data:
    print(f"{entity_id}: {text}")
