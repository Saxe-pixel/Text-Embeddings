

from dotenv import load_dotenv
import os

load_dotenv()

print("🔑 Token:", os.getenv("ASTRA_DB_APPLICATION_TOKEN"))
print("🧠 DB ID:", os.getenv("ASTRA_DB_ID"))
print("📦 Keyspace:", os.getenv("ASTRA_DB_KEYSPACE"))


