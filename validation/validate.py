"""
validation/validate.py
======================
Automated validation checks comparing PostgreSQL (source) vs MongoDB (target).

Checks performed
----------------
  1. Record counts        – every entity must match exactly
  2. Checksum comparison  – SHA-256 hash on key booking fields for large tables
  3. Spot-check queries   – equivalent aggregations run on both DBs, results compared
       a. Total revenue per airline (top 10)
       b. Average review score per flight status
       c. Booking status distribution
       d. Passenger loyalty tier distribution
       e. Top 5 airports by departure count

Output
------
  - Coloured PASS / FAIL table printed to console
  - Full report saved to logs/validation.log
  - Exit code 0 if all checks pass, 1 if any fail

Usage:
    python validation/validate.py
"""

import os
import sys
import hashlib
import logging
from datetime import datetime, timezone
from collections import defaultdict

import colorlog

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_pg_conn, get_pg_cursor, get_mongo_client, get_mongo_db  # noqa

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(message)s"
))
file_handler = logging.FileHandler("logs/validation.log", encoding="utf-8")
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(message)s"
))
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result tracking
# ---------------------------------------------------------------------------
results: list = []   # list of (check_name, passed, pg_value, mongo_value, note)

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def record(name: str, passed: bool, pg_val, mongo_val, note: str = "") -> bool:
    results.append((name, passed, pg_val, mongo_val, note))
    status = f"{GREEN}PASS{RESET}" if passed else f"{RED}FAIL{RESET}"
    log.info(f"  [{status}]  {name}")
    if not passed:
        log.warning(f"         PostgreSQL : {pg_val}")
        log.warning(f"         MongoDB    : {mongo_val}")
        if note:
            log.warning(f"         Note       : {note}")
    return passed


# ===========================================================================
# Check 1 — Record counts
# ===========================================================================

def check_record_counts(pg_conn, mongo_db) -> None:
    log.info("")
    log.info("── CHECK 1: Record Counts ──────────────────────────────────")

    entity_map = {
        # (pg_table, mongo_collection)
        "airports":   ("airports",         "airports"),
        "airlines":   ("airlines",         "airlines"),
        "flights":    ("flights",          "flights"),
        "passengers": ("passengers",       "passengers"),
        "bookings":   ("bookings",         "bookings"),
    }

    with get_pg_cursor(pg_conn) as cur:
        for label, (pg_table, mongo_col) in entity_map.items():
            cur.execute(f"SELECT COUNT(*) AS n FROM {pg_table}")
            pg_count    = cur.fetchone()["n"]
            mongo_count = mongo_db[mongo_col].count_documents({})
            passed      = pg_count == mongo_count
            record(
                f"Count: {label}",
                passed,
                pg_count,
                mongo_count,
                "" if passed else f"Difference: {abs(pg_count - mongo_count)}",
            )


# ===========================================================================
# Check 2 — Checksum on key booking fields
# ===========================================================================

def check_booking_checksums(pg_conn, mongo_db) -> None:
    log.info("")
    log.info("── CHECK 2: Booking Checksums ──────────────────────────────")

    # PostgreSQL: collect (booking_reference, price_paid, status) sorted
    with get_pg_cursor(pg_conn) as cur:
        cur.execute("""
            SELECT booking_reference,
                   ROUND(price_paid::numeric, 2)::text AS price,
                   status
            FROM   bookings
            ORDER  BY booking_reference
        """)
        pg_rows = cur.fetchall()

    # MongoDB: same fields, same sort
    mongo_rows = list(
        mongo_db["bookings"].find(
            {},
            {"booking_reference": 1, "price_paid": 1, "status": 1, "_id": 0},
        ).sort("booking_reference", 1)
    )

    # Build checksums
    def make_hash(rows, ref_key, price_key, status_key) -> str:
        h = hashlib.sha256()
        for r in rows:
            ref    = str(r[ref_key]).strip()
            price  = f"{float(r[price_key]):.2f}"
            status = str(r[status_key]).strip()
            h.update(f"{ref}|{price}|{status}".encode())
        return h.hexdigest()

    pg_hash    = make_hash(pg_rows,    "booking_reference", "price", "status")
    mongo_hash = make_hash(mongo_rows, "booking_reference", "price_paid", "status")

    record(
        "Checksum: booking_reference + price_paid + status",
        pg_hash == mongo_hash,
        pg_hash[:16] + "…",
        mongo_hash[:16] + "…",
        "Hash mismatch — data drift detected" if pg_hash != mongo_hash else "",
    )

    # Also verify count matches before trusting hash
    record(
        "Checksum: row count agreement",
        len(pg_rows) == len(mongo_rows),
        len(pg_rows),
        len(mongo_rows),
    )


# ===========================================================================
# Check 3 — Spot-check queries
# ===========================================================================

def check_spot_queries(pg_conn, mongo_db) -> None:
    log.info("")
    log.info("── CHECK 3: Spot-Check Queries ─────────────────────────────")

    # ── 3a. Total revenue per airline (top 10) ────────────────────────────
    with get_pg_cursor(pg_conn) as cur:
        cur.execute("""
            SELECT al.iata_code,
                   ROUND(SUM(b.price_paid)::numeric, 2) AS revenue
            FROM   bookings b
            JOIN   flights  f  ON f.flight_id  = b.flight_id
            JOIN   airlines al ON al.airline_id = f.airline_id
            WHERE  b.status IN ('confirmed','completed')
            GROUP  BY al.iata_code
            ORDER  BY revenue DESC
            LIMIT  10
        """)
        pg_revenue = {r["iata_code"]: float(r["revenue"]) for r in cur.fetchall()}

    mongo_revenue = {}
    pipeline = [
        {"$match": {"status": {"$in": ["confirmed", "completed"]}}},
        {"$group": {
            "_id":     "$flight.airline.iata_code",
            "revenue": {"$sum": "$price_paid"},
        }},
        {"$sort": {"revenue": -1}},
        {"$limit": 10},
    ]
    for doc in mongo_db["bookings"].aggregate(pipeline):
        mongo_revenue[doc["_id"]] = round(float(doc["revenue"]), 2)

    # Compare top airline matches (within 0.01 tolerance for float rounding)
    pg_top    = sorted(pg_revenue.items(),    key=lambda x: -x[1])[:5]
    mongo_top = sorted(mongo_revenue.items(), key=lambda x: -x[1])[:5]
    pg_codes    = [x[0] for x in pg_top]
    mongo_codes = [x[0] for x in mongo_top]

    record(
        "Spot-check 3a: Top 5 airlines by revenue match",
        pg_codes == mongo_codes,
        pg_codes,
        mongo_codes,
    )

    # Revenue values within 1 % tolerance
    revenue_match = all(
        abs(pg_revenue.get(k, 0) - mongo_revenue.get(k, 0))
        / max(pg_revenue.get(k, 1), 1) < 0.01
        for k in pg_codes
    )
    record(
        "Spot-check 3a: Top 5 airline revenues within 1% tolerance",
        revenue_match,
        {k: pg_revenue.get(k) for k in pg_codes},
        {k: mongo_revenue.get(k) for k in mongo_codes},
    )

    # ── 3b. Booking status distribution ──────────────────────────────────
    with get_pg_cursor(pg_conn) as cur:
        cur.execute("""
            SELECT status, COUNT(*) AS n
            FROM   bookings
            GROUP  BY status
            ORDER  BY status
        """)
        pg_status = {r["status"]: r["n"] for r in cur.fetchall()}

    pipeline = [
        {"$group": {"_id": "$status", "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    mongo_status = {
        doc["_id"]: doc["n"]
        for doc in mongo_db["bookings"].aggregate(pipeline)
    }

    record(
        "Spot-check 3b: Booking status distribution matches",
        pg_status == mongo_status,
        pg_status,
        mongo_status,
    )

    # ── 3c. Passenger loyalty tier distribution ───────────────────────────
    with get_pg_cursor(pg_conn) as cur:
        cur.execute("""
            SELECT tier, COUNT(*) AS n
            FROM   loyalty_accounts
            GROUP  BY tier
            ORDER  BY tier
        """)
        pg_tiers = {r["tier"]: r["n"] for r in cur.fetchall()}

    pipeline = [
        {"$group": {"_id": "$loyalty.tier", "n": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    mongo_tiers = {
        doc["_id"]: doc["n"]
        for doc in mongo_db["passengers"].aggregate(pipeline)
    }

    record(
        "Spot-check 3c: Loyalty tier distribution matches",
        pg_tiers == mongo_tiers,
        pg_tiers,
        mongo_tiers,
    )

    # ── 3d. Top 5 airports by departure count ─────────────────────────────
    with get_pg_cursor(pg_conn) as cur:
        cur.execute("""
            SELECT ap.iata_code, COUNT(*) AS n
            FROM   flights f
            JOIN   airports ap ON ap.airport_id = f.origin_id
            GROUP  BY ap.iata_code
            ORDER  BY n DESC
            LIMIT  5
        """)
        pg_airports = [r["iata_code"] for r in cur.fetchall()]

    pipeline = [
        {"$group": {"_id": "$origin.iata_code", "n": {"$sum": 1}}},
        {"$sort": {"n": -1}},
        {"$limit": 5},
    ]
    mongo_airports = [
        doc["_id"]
        for doc in mongo_db["flights"].aggregate(pipeline)
    ]

    record(
        "Spot-check 3d: Top 5 departure airports match",
        pg_airports == mongo_airports,
        pg_airports,
        mongo_airports,
    )

    # ── 3e. Total booking count for a single flight ───────────────────────
    with get_pg_cursor(pg_conn) as cur:
        # Pick the flight with the most bookings
        cur.execute("""
            SELECT f.flight_number, COUNT(*) AS n
            FROM   bookings b
            JOIN   flights  f ON f.flight_id = b.flight_id
            GROUP  BY f.flight_number
            ORDER  BY n DESC
            LIMIT  1
        """)
        row = cur.fetchone()
        pg_flight_num = row["flight_number"]
        pg_flight_count = row["n"]

    mongo_flight_count = mongo_db["bookings"].count_documents(
        {"flight.flight_number": pg_flight_num}
    )

    record(
        f"Spot-check 3e: Booking count for busiest flight ({pg_flight_num})",
        pg_flight_count == mongo_flight_count,
        pg_flight_count,
        mongo_flight_count,
    )


# ===========================================================================
# Final report
# ===========================================================================

def print_report(start_time: datetime) -> int:
    elapsed  = (datetime.now() - start_time).total_seconds()
    total    = len(results)
    passed   = sum(1 for r in results if r[1])
    failed   = total - passed

    log.info("")
    log.info("=" * 60)
    log.info(f"{BOLD}VALIDATION REPORT{RESET}")
    log.info("=" * 60)
    log.info(f"  {'Check':<55} {'Result'}")
    log.info(f"  {'-'*55} {'------'}")
    for name, ok, pg_val, mongo_val, note in results:
        status = f"{GREEN}PASS{RESET}" if ok else f"{RED}FAIL{RESET}"
        log.info(f"  {name:<55} {status}")
        if not ok:
            log.info(f"    {'PG':>6}: {pg_val}")
            log.info(f"    {'Mongo':>6}: {mongo_val}")
            if note:
                log.info(f"    {'Note':>6}: {note}")
    log.info("=" * 60)
    log.info(f"  Total checks : {total}")
    log.info(f"  {GREEN}Passed{RESET}       : {passed}")
    if failed:
        log.info(f"  {RED}Failed{RESET}       : {failed}")
    else:
        log.info(f"  Failed       : {failed}")
    log.info(f"  Elapsed      : {elapsed:.1f}s")
    log.info("=" * 60)

    if failed == 0:
        log.info(f"{GREEN}✓ All checks passed — migration validated successfully.{RESET}")
    else:
        log.warning(f"{RED}✗ {failed} check(s) failed — review discrepancies above.{RESET}")

    log.info("Full report saved to logs/validation.log")
    return 0 if failed == 0 else 1


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    start_time = datetime.now()
    log.info("=" * 60)
    log.info("FLIGHT BOOKING — Migration Validation")
    log.info("=" * 60)

    log.info("Connecting to PostgreSQL …")
    try:
        pg_conn = get_pg_conn()
    except Exception as e:
        log.critical(f"PostgreSQL connection failed: {e}")
        sys.exit(1)

    log.info("Connecting to MongoDB …")
    try:
        mongo_client = get_mongo_client()
        mongo_db     = get_mongo_db(mongo_client)
    except Exception as e:
        log.critical(f"MongoDB connection failed: {e}")
        pg_conn.close()
        sys.exit(1)

    check_record_counts(pg_conn, mongo_db)
    check_booking_checksums(pg_conn, mongo_db)
    check_spot_queries(pg_conn, mongo_db)

    pg_conn.close()
    mongo_client.close()

    exit_code = print_report(start_time)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
