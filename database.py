# database.py
# Central database connection for the entire system.
# Every model and script imports from here — never create
# a separate connection elsewhere.

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise ValueError("DATABASE_URL is not set in .env file")

# Convert psycopg3 style URL to SQLAlchemy compatible format
# SQLAlchemy needs postgresql+psycopg:// not postgresql://
db_url = DATABASE_URL
if db_url.startswith("postgresql://"):
    db_url = db_url.replace(
        "postgresql://", "postgresql+psycopg://", 1
    )

# Create the engine
# pool_pre_ping=True means SQLAlchemy checks the connection
# is alive before using it — prevents stale connection errors
# pool_size=5 means max 5 connections open at once
# max_overflow=10 means up to 10 extra connections under load
engine = create_engine(
    db_url,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
    echo=False  # Set to True temporarily if you want to see SQL queries
)

# SessionLocal is the factory for database sessions
# Each request to your API gets its own session
# autocommit=False means changes only save when you explicitly commit
# autoflush=False means SQLAlchemy won't auto-send pending changes
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

# Base is the parent class for all your database models
# Every model file imports Base from here
Base = declarative_base()


def get_db():
    """
    Dependency function for FastAPI endpoints.
    Creates a database session, yields it to the endpoint,
    then closes it automatically when the request is done.
    Always use this in FastAPI routes — never create sessions manually.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def verify_connection():
    """
    Tests that the database connection is working.
    Called on application startup.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Database connection verified successfully")
        return True
    except Exception as e:
        print(f"Database connection failed: {e}")
        return False
