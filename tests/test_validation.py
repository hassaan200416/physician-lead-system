from dotenv import load_dotenv
import os
import psycopg

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

try:
    conn = psycopg.connect(DATABASE_URL)
    print("SUCCESS — Connected to Supabase PostgreSQL")
    print(f"Server version: {conn.info.server_version}")
    conn.close()
except Exception as e:
    print(f"FAILED — {e}")