# test_neon_connection.py
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATABASE_URI = os.getenv("DATABASE_URI")
engine = create_engine(DATABASE_URI)

try:
    with engine.connect() as conn:
        result = conn.execute(text("SELECT NOW()"))
        for row in result:
            print("✅ Connected to Neon DB! Current time:", row[0])
except Exception as e:
    print("❌ Failed to connect:", e)
