"""
db.py — shared connection helpers for PostgreSQL and MongoDB.
Import this module from any script in the project.
"""

import os
import logging
from dotenv import load_dotenv
import psycopg2
import psycopg2.extras
from pymongo import MongoClient

load_dotenv()

log = logging.getLogger(__name__)


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def get_pg_conn():
    """Return a psycopg2 connection to PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", 5432)),
        dbname=os.getenv("PG_DB", "flight_booking"),
        user=os.getenv("PG_USER", "flight_user"),
        password=os.getenv("PG_PASS", "flight_pass"),
    )
    conn.autocommit = False
    log.debug("PostgreSQL connection established")
    return conn


def get_pg_cursor(conn):
    """Return a DictCursor so rows can be accessed by column name."""
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ── MongoDB ───────────────────────────────────────────────────────────────────

def get_mongo_client():
    """Return an authenticated MongoClient."""
    host = os.getenv("MONGO_HOST", "localhost")
    port = int(os.getenv("MONGO_PORT", 27017))
    user = os.getenv("MONGO_USER", "mongo_user")
    password = os.getenv("MONGO_PASS", "mongo_pass")
    uri = f"mongodb://{user}:{password}@{host}:{port}/"
    client = MongoClient(uri, serverSelectionTimeoutMS=5000)
    # Trigger a real connection attempt so errors surface early
    client.admin.command("ping")
    log.debug("MongoDB connection established")
    return client


def get_mongo_db(client=None):
    """Return the flight_nosql database handle."""
    if client is None:
        client = get_mongo_client()
    db_name = os.getenv("MONGO_DB", "flight_nosql")
    return client[db_name]
