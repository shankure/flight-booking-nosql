"""
migration/migrate.py
====================
Migrates data from PostgreSQL (flight_booking) to MongoDB (flight_nosql).

Collections produced
---------------------
  bookings        – one document per booking, fully denormalized
  flights         – one document per flight with aggregated stats
  passengers      – one document per passenger with loyalty info
  airlines        – one document per airline with route/revenue stats
  airports        – one document per airport

Derived / aggregated fields (≥2 required by spec)
---------------------------------------------------
  1. loyalty_tier          – bronze/silver/gold/platinum derived from total_flights
  2. total_spent           – sum of price_paid per passenger
  3. occupancy_rate        – bookings confirmed+completed / total_seats (per flight)
  4. revenue_per_km        – total flight revenue / route distance_km
  5. avg_review_score      – mean rating across all reviews for a flight
  6. on_time_rate          – % of airline flights with no delay record
  7. price_category        – budget/standard/premium derived from base_price

Idempotency
-----------
  Every insert uses update_one(..., upsert=True) keyed on a stable natural ID.
  Running the script twice produces identical results — no duplicates.

Error handling
--------------
  1. Malformed / NULL rows are caught per-record and logged; migration continues.
  2. Connection failures are caught at startup with a clear message.
  3. A full summary (processed / skipped / errors) is printed at the end.
  4. All errors are also written to logs/migration.log.

Usage:
    python migration/migrate.py
"""

import os
import sys
import json
import logging
import hashlib
from datetime import datetime, timezone
from collections import defaultdict

import colorlog
from tqdm import tqdm
from pymongo import UpdateOne
from pymongo.errors import BulkWriteError, ConnectionFailure

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_pg_conn, get_pg_cursor, get_mongo_client, get_mongo_db  # noqa

# ---------------------------------------------------------------------------
# Logging — console (coloured) + file
# ---------------------------------------------------------------------------
os.makedirs("logs", exist_ok=True)

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(message)s"
))
file_handler = logging.FileHandler("logs/migration.log", encoding="utf-8")
file_handler.setFormatter(logging.Formatter(
    "%(asctime)s  %(levelname)-8s  %(message)s"
))
logging.basicConfig(level=logging.INFO, handlers=[console_handler, file_handler])
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BATCH_SIZE = 500   # upsert batch size for pymongo bulk_write


# ===========================================================================
# Helpers
# ===========================================================================

def to_float(val, default=0.0) -> float:
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def to_int(val, default=0) -> int:
    try:
        return int(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def derive_loyalty_tier(total_flights: int) -> str:
    """Derived field 1 — loyalty tier from flight count."""
    if total_flights >= 50:  return "platinum"
    if total_flights >= 20:  return "gold"
    if total_flights >= 5:   return "silver"
    return "bronze"


def derive_price_category(base_price: float) -> str:
    """Derived field 7 — price category from base ticket price."""
    if base_price < 100:   return "budget"
    if base_price < 400:   return "standard"
    return "premium"


def bulk_upsert(collection, operations: list, stats: dict) -> None:
    """Execute a list of UpdateOne upsert operations in one bulk_write call."""
    if not operations:
        return
    try:
        result = collection.bulk_write(operations, ordered=False)
        stats["inserted"] += result.upserted_count
        stats["updated"]  += result.modified_count
    except BulkWriteError as e:
        log.error(f"Bulk write error: {e.details}")
        stats["errors"] += len(e.details.get("writeErrors", []))


# ===========================================================================
# PostgreSQL data loaders
# ===========================================================================

def fetch_airports(conn) -> dict:
    log.info("Loading airports from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT airport_id, iata_code, name, city, country,
                   latitude, longitude, timezone
            FROM   airports
        """)
        rows = cur.fetchall()
    return {r["airport_id"]: dict(r) for r in rows}


def fetch_airlines(conn) -> dict:
    log.info("Loading airlines from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT airline_id, iata_code, name, country, active
            FROM   airlines
        """)
        rows = cur.fetchall()
    return {r["airline_id"]: dict(r) for r in rows}


def fetch_aircraft(conn) -> dict:
    log.info("Loading aircraft from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT aircraft_id, model, manufacturer, total_seats, range_km
            FROM   aircraft
        """)
        rows = cur.fetchall()
    return {r["aircraft_id"]: dict(r) for r in rows}


def fetch_flights(conn) -> dict:
    log.info("Loading flights from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT f.flight_id, f.flight_number, f.airline_id, f.aircraft_id,
                   f.origin_id, f.destination_id,
                   f.scheduled_departure, f.scheduled_arrival,
                   f.base_price, f.total_seats, f.status,
                   r.distance_km
            FROM   flights f
            LEFT JOIN routes r
                   ON r.airline_id    = f.airline_id
                  AND r.origin_id     = f.origin_id
                  AND r.destination_id = f.destination_id
        """)
        rows = cur.fetchall()
    return {r["flight_id"]: dict(r) for r in rows}


def fetch_passengers(conn) -> dict:
    log.info("Loading passengers from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT passenger_id, first_name, last_name, email,
                   phone, date_of_birth, nationality, passport_number,
                   created_at
            FROM   passengers
        """)
        rows = cur.fetchall()
    return {r["passenger_id"]: dict(r) for r in rows}


def fetch_bookings(conn) -> list:
    log.info("Loading bookings from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT booking_id, booking_reference, passenger_id, flight_id,
                   seat_number, cabin_class, price_paid, booking_date, status
            FROM   bookings
        """)
        return [dict(r) for r in cur.fetchall()]


def fetch_reviews(conn) -> dict:
    """Returns {booking_id: review_dict}."""
    log.info("Loading reviews from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT review_id, booking_id, rating, title, body, created_at
            FROM   reviews
        """)
        return {r["booking_id"]: dict(r) for r in cur.fetchall()}


def fetch_loyalty(conn) -> dict:
    """Returns {passenger_id: loyalty_dict}."""
    log.info("Loading loyalty accounts from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT loyalty_id, passenger_id, tier, points,
                   total_flights, total_spent, joined_at
            FROM   loyalty_accounts
        """)
        return {r["passenger_id"]: dict(r) for r in cur.fetchall()}


def fetch_delays(conn) -> dict:
    """Returns {flight_id: delay_dict}."""
    log.info("Loading flight delays from PostgreSQL …")
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT delay_id, flight_id, delay_minutes, reason, reported_at
            FROM   flight_delays
        """)
        return {r["flight_id"]: dict(r) for r in cur.fetchall()}


# ===========================================================================
# Aggregation helpers  (run once, used across collections)
# ===========================================================================

def build_flight_aggs(bookings: list, reviews: dict) -> dict:
    """
    Per flight_id aggregations:
      - confirmed_count, completed_count, total_revenue
      - avg_review_score  (derived field 5)
      - occupancy_rate    (derived field 3)
    """
    agg = defaultdict(lambda: {
        "confirmed": 0, "completed": 0,
        "revenue": 0.0, "ratings": [],
    })
    for b in bookings:
        fid = b["flight_id"]
        agg[fid]["revenue"] += to_float(b["price_paid"])
        if b["status"] == "confirmed":
            agg[fid]["confirmed"] += 1
        elif b["status"] == "completed":
            agg[fid]["completed"] += 1
        rev = reviews.get(b["booking_id"])
        if rev:
            agg[fid]["ratings"].append(to_int(rev["rating"]))
    return agg


def build_passenger_aggs(bookings: list) -> dict:
    """Per passenger_id: total_flights, total_spent (derived field 2)."""
    agg = defaultdict(lambda: {"flights": 0, "spent": 0.0})
    for b in bookings:
        if b["status"] in ("confirmed", "completed"):
            pid = b["passenger_id"]
            agg[pid]["flights"] += 1
            agg[pid]["spent"]   += to_float(b["price_paid"])
    return agg


def build_airline_aggs(flights: dict, bookings: list,
                       delays: dict) -> dict:
    """
    Per airline_id:
      - total_routes, total_revenue
      - on_time_rate  (derived field 6)
    """
    agg = defaultdict(lambda: {
        "flight_ids": set(), "revenue": 0.0,
        "delayed_flights": 0,
    })
    # Revenue from bookings
    flight_revenue = defaultdict(float)
    for b in bookings:
        flight_revenue[b["flight_id"]] += to_float(b["price_paid"])

    for fid, f in flights.items():
        aid = f["airline_id"]
        agg[aid]["flight_ids"].add(fid)
        agg[aid]["revenue"] += flight_revenue.get(fid, 0.0)
        if fid in delays:
            agg[aid]["delayed_flights"] += 1

    return agg


# ===========================================================================
# Collection migrators
# ===========================================================================

def migrate_airports(mongo_db, airports: dict) -> None:
    log.info("Migrating airports …")
    collection = mongo_db["airports"]
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    ops   = []

    for ap in tqdm(airports.values(), desc="  airports", unit="doc"):
        try:
            doc = {
                "iata_code":  ap["iata_code"],
                "name":       ap["name"],
                "city":       ap["city"],
                "country":    ap["country"],
                "location": {
                    "type":        "Point",
                    "coordinates": [
                        to_float(ap["longitude"]),
                        to_float(ap["latitude"]),
                    ],
                },
                "timezone":        ap["timezone"],
                "_migrated_at":    datetime.now(timezone.utc),
            }
            ops.append(UpdateOne(
                {"iata_code": ap["iata_code"]},
                {"$set": doc},
                upsert=True,
            ))
            if len(ops) >= BATCH_SIZE:
                bulk_upsert(collection, ops, stats)
                ops = []
        except Exception as e:
            log.error(f"Airport {ap.get('iata_code')} skipped: {e}")
            stats["errors"] += 1

    bulk_upsert(collection, ops, stats)
    collection.create_index("iata_code", unique=True)
    log.info(f"  airports → inserted={stats['inserted']}  "
             f"updated={stats['updated']}  errors={stats['errors']}")


def migrate_airlines(mongo_db, airlines: dict, flights: dict,
                     bookings: list, delays: dict) -> None:
    log.info("Migrating airlines …")
    collection  = mongo_db["airlines"]
    airline_agg = build_airline_aggs(flights, bookings, delays)
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    ops   = []

    for al in tqdm(airlines.values(), desc="  airlines", unit="doc"):
        try:
            aid = al["airline_id"]
            agg = airline_agg[aid]
            total_flights  = len(agg["flight_ids"])
            delayed        = agg["delayed_flights"]

            # Derived field 6 — on_time_rate
            on_time_rate = round(
                (1 - delayed / total_flights) * 100 if total_flights else 0.0, 2
            )

            doc = {
                "iata_code":     al["iata_code"],
                "name":          al["name"],
                "country":       al["country"],
                "active":        al["active"],
                "stats": {
                    "total_flights":   total_flights,
                    "total_revenue":   round(agg["revenue"], 2),
                    "delayed_flights": delayed,
                    "on_time_rate":    on_time_rate,   # derived field 6
                },
                "_migrated_at": datetime.now(timezone.utc),
            }
            ops.append(UpdateOne(
                {"iata_code": al["iata_code"]},
                {"$set": doc},
                upsert=True,
            ))
            if len(ops) >= BATCH_SIZE:
                bulk_upsert(collection, ops, stats)
                ops = []
        except Exception as e:
            log.error(f"Airline {al.get('iata_code')} skipped: {e}")
            stats["errors"] += 1

    bulk_upsert(collection, ops, stats)
    collection.create_index("iata_code", unique=True)
    log.info(f"  airlines → inserted={stats['inserted']}  "
             f"updated={stats['updated']}  errors={stats['errors']}")


def migrate_flights(mongo_db, flights: dict, airlines: dict,
                    airports: dict, aircraft: dict,
                    bookings: list, reviews: dict,
                    delays: dict) -> None:
    log.info("Migrating flights …")
    collection = mongo_db["flights"]
    flight_agg = build_flight_aggs(bookings, reviews)
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    ops   = []

    for f in tqdm(flights.values(), desc="  flights", unit="doc"):
        try:
            fid  = f["flight_id"]
            agg  = flight_agg[fid]
            al   = airlines.get(f["airline_id"], {})
            orig = airports.get(f["origin_id"], {})
            dest = airports.get(f["destination_id"], {})
            ac   = aircraft.get(f["aircraft_id"], {})
            dly  = delays.get(fid)

            total_seats  = to_int(f["total_seats"], 1)
            active_books = agg["confirmed"] + agg["completed"]

            # Derived field 3 — occupancy_rate
            occupancy_rate = round(active_books / total_seats * 100, 2)

            # Derived field 4 — revenue_per_km
            dist_km = to_int(f.get("distance_km"), 0)
            revenue_per_km = round(
                agg["revenue"] / dist_km if dist_km > 0 else 0.0, 4
            )

            # Derived field 5 — avg_review_score
            ratings = agg["ratings"]
            avg_review_score = round(
                sum(ratings) / len(ratings) if ratings else 0.0, 2
            )

            # Derived field 7 — price_category
            price_category = derive_price_category(to_float(f["base_price"]))

            doc = {
                "flight_number": f["flight_number"],
                "status":        f["status"],
                "airline": {
                    "airline_id": f["airline_id"],
                    "iata_code":  al.get("iata_code"),
                    "name":       al.get("name"),
                },
                "aircraft": {
                    "aircraft_id":  f["aircraft_id"],
                    "model":        ac.get("model"),
                    "manufacturer": ac.get("manufacturer"),
                    "total_seats":  total_seats,
                },
                "origin": {
                    "airport_id": f["origin_id"],
                    "iata_code":  orig.get("iata_code"),
                    "name":       orig.get("name"),
                    "city":       orig.get("city"),
                    "country":    orig.get("country"),
                },
                "destination": {
                    "airport_id": f["destination_id"],
                    "iata_code":  dest.get("iata_code"),
                    "name":       dest.get("name"),
                    "city":       dest.get("city"),
                    "country":    dest.get("country"),
                },
                "schedule": {
                    "departure": f["scheduled_departure"],
                    "arrival":   f["scheduled_arrival"],
                },
                "pricing": {
                    "base_price":     to_float(f["base_price"]),
                    "price_category": price_category,          # derived field 7
                },
                "delay": {
                    "delay_minutes": to_int(dly["delay_minutes"]) if dly else 0,
                    "reason":        dly["reason"] if dly else None,
                } if dly else None,
                "stats": {
                    "total_bookings":   active_books,
                    "total_revenue":    round(agg["revenue"], 2),
                    "occupancy_rate":   occupancy_rate,     # derived field 3
                    "revenue_per_km":   revenue_per_km,     # derived field 4
                    "avg_review_score": avg_review_score,   # derived field 5
                    "distance_km":      dist_km,
                },
                "_migrated_at": datetime.now(timezone.utc),
            }
            ops.append(UpdateOne(
                {"flight_number": f["flight_number"],
                 "schedule.departure": f["scheduled_departure"]},
                {"$set": doc},
                upsert=True,
            ))
            if len(ops) >= BATCH_SIZE:
                bulk_upsert(collection, ops, stats)
                ops = []
        except Exception as e:
            log.error(f"Flight {f.get('flight_id')} skipped: {e}")
            stats["errors"] += 1

    bulk_upsert(collection, ops, stats)
    collection.create_index(
        [("flight_number", 1), ("schedule.departure", 1)], unique=True
    )
    collection.create_index("airline.iata_code")
    collection.create_index("status")
    log.info(f"  flights → inserted={stats['inserted']}  "
             f"updated={stats['updated']}  errors={stats['errors']}")


def migrate_passengers(mongo_db, passengers: dict, loyalty: dict,
                       bookings: list) -> None:
    log.info("Migrating passengers …")
    collection   = mongo_db["passengers"]
    pax_agg      = build_passenger_aggs(bookings)
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    ops   = []

    for p in tqdm(passengers.values(), desc="  passengers", unit="doc"):
        try:
            pid  = p["passenger_id"]
            loy  = loyalty.get(pid, {})
            agg  = pax_agg[pid]

            total_flights = to_int(loy.get("total_flights") or agg["flights"])
            total_spent   = round(
                to_float(loy.get("total_spent") or agg["spent"]), 2
            )

            # Derived field 1 — loyalty_tier (recomputed from actual flight count)
            loyalty_tier = derive_loyalty_tier(total_flights)

            # Derived field 2 — total_spent (aggregated from bookings)
            doc = {
                "first_name":      p["first_name"],
                "last_name":       p["last_name"],
                "email":           p["email"],
                "phone":           p.get("phone"),
                "date_of_birth":   p["date_of_birth"].isoformat()
                                   if hasattr(p["date_of_birth"], "isoformat")
                                   else str(p["date_of_birth"]),
                "nationality":     p["nationality"],
                "passport_number": p["passport_number"],
                "loyalty": {
                    "tier":          loyalty_tier,        # derived field 1
                    "points":        to_int(loy.get("points")),
                    "total_flights": total_flights,
                    "total_spent":   total_spent,         # derived field 2
                    "joined_at":     loy.get("joined_at"),
                },
                "_migrated_at": datetime.now(timezone.utc),
            }
            ops.append(UpdateOne(
                {"email": p["email"]},
                {"$set": doc},
                upsert=True,
            ))
            if len(ops) >= BATCH_SIZE:
                bulk_upsert(collection, ops, stats)
                ops = []
        except Exception as e:
            log.error(f"Passenger {p.get('passenger_id')} skipped: {e}")
            stats["errors"] += 1

    bulk_upsert(collection, ops, stats)
    collection.create_index("email", unique=True)
    collection.create_index("passport_number")
    log.info(f"  passengers → inserted={stats['inserted']}  "
             f"updated={stats['updated']}  errors={stats['errors']}")


def migrate_bookings(mongo_db, bookings: list, passengers: dict,
                     flights: dict, airlines: dict, airports: dict,
                     loyalty: dict, reviews: dict) -> None:
    log.info("Migrating bookings …")
    collection = mongo_db["bookings"]
    stats = {"inserted": 0, "updated": 0, "errors": 0}
    ops   = []

    for b in tqdm(bookings, desc="  bookings", unit="doc"):
        try:
            # ── error scenario 1: missing foreign key references ──────────────
            p = passengers.get(b["passenger_id"])
            f = flights.get(b["flight_id"])
            if p is None or f is None:
                log.warning(
                    f"Booking {b['booking_reference']} skipped: "
                    f"missing passenger or flight reference"
                )
                stats["errors"] += 1
                continue

            # ── error scenario 2: malformed price ────────────────────────────
            try:
                price_paid = float(b["price_paid"])
                if price_paid < 0:
                    raise ValueError("negative price")
            except (TypeError, ValueError) as price_err:
                log.warning(
                    f"Booking {b['booking_reference']} skipped: "
                    f"bad price_paid value — {price_err}"
                )
                stats["errors"] += 1
                continue

            al   = airlines.get(f["airline_id"], {})
            orig = airports.get(f["origin_id"], {})
            dest = airports.get(f["destination_id"], {})
            loy  = loyalty.get(b["passenger_id"], {})
            rev  = reviews.get(b["booking_id"])

            total_flights = to_int(loy.get("total_flights"))
            loyalty_tier  = derive_loyalty_tier(total_flights)  # derived field 1

            doc = {
                "booking_reference": b["booking_reference"],
                "status":            b["status"],
                "booking_date":      b["booking_date"],
                "seat_number":       b["seat_number"],
                "cabin_class":       b["cabin_class"],
                "price_paid":        price_paid,

                # Embedded passenger (denormalized)
                "passenger": {
                    "passenger_id":    b["passenger_id"],
                    "first_name":      p["first_name"],
                    "last_name":       p["last_name"],
                    "email":           p["email"],
                    "nationality":     p["nationality"],
                    "passport_number": p["passport_number"],
                    "loyalty_tier":    loyalty_tier,       # derived field 1
                },

                # Embedded flight summary (denormalized)
                "flight": {
                    "flight_id":     b["flight_id"],
                    "flight_number": f["flight_number"],
                    "status":        f["status"],
                    "departure":     f["scheduled_departure"],
                    "arrival":       f["scheduled_arrival"],
                    "airline": {
                        "iata_code": al.get("iata_code"),
                        "name":      al.get("name"),
                    },
                    "origin": {
                        "iata_code": orig.get("iata_code"),
                        "city":      orig.get("city"),
                        "country":   orig.get("country"),
                    },
                    "destination": {
                        "iata_code": dest.get("iata_code"),
                        "city":      dest.get("city"),
                        "country":   dest.get("country"),
                    },
                },

                # Embedded review (if exists)
                "review": {
                    "rating":     to_int(rev["rating"]),
                    "title":      rev.get("title"),
                    "body":       rev.get("body"),
                    "created_at": rev.get("created_at"),
                } if rev else None,

                "_migrated_at": datetime.now(timezone.utc),
            }
            ops.append(UpdateOne(
                {"booking_reference": b["booking_reference"]},
                {"$set": doc},
                upsert=True,
            ))
            if len(ops) >= BATCH_SIZE:
                bulk_upsert(collection, ops, stats)
                ops = []
        except Exception as e:
            log.error(f"Booking {b.get('booking_reference')} skipped: {e}")
            stats["errors"] += 1

    bulk_upsert(collection, ops, stats)
    collection.create_index("booking_reference", unique=True)
    collection.create_index("passenger.email")
    collection.create_index("flight.flight_number")
    collection.create_index("status")
    log.info(f"  bookings → inserted={stats['inserted']}  "
             f"updated={stats['updated']}  errors={stats['errors']}")


# ===========================================================================
# Summary
# ===========================================================================

def print_summary(mongo_db, start_time: datetime) -> None:
    elapsed = (datetime.now() - start_time).total_seconds()
    collections = ["airports", "airlines", "flights", "passengers", "bookings"]
    log.info("=" * 55)
    log.info("MIGRATION SUMMARY")
    log.info("=" * 55)
    for name in collections:
        count = mongo_db[name].count_documents({})
        log.info(f"  {name:<20} {count:>8,} documents")
    log.info(f"  {'Elapsed':<20} {elapsed:>8.1f} seconds")
    log.info("=" * 55)
    log.info("Derived fields computed:")
    log.info("  1. loyalty_tier        (bronze/silver/gold/platinum)")
    log.info("  2. total_spent         (sum of price_paid per passenger)")
    log.info("  3. occupancy_rate      (active bookings / total seats)")
    log.info("  4. revenue_per_km      (revenue / route distance)")
    log.info("  5. avg_review_score    (mean rating per flight)")
    log.info("  6. on_time_rate        (% flights with no delay)")
    log.info("  7. price_category      (budget / standard / premium)")
    log.info("=" * 55)


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    start_time = datetime.now()
    log.info("=" * 55)
    log.info("FLIGHT BOOKING — PostgreSQL → MongoDB Migration")
    log.info("=" * 55)

    # ── Connect ──────────────────────────────────────────────────────────────
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
    except ConnectionFailure as e:
        log.critical(f"MongoDB connection failed: {e}")
        pg_conn.close()
        sys.exit(1)

    # ── Load all PostgreSQL data into memory ─────────────────────────────────
    airports   = fetch_airports(pg_conn)
    airlines   = fetch_airlines(pg_conn)
    aircraft   = fetch_aircraft(pg_conn)
    flights    = fetch_flights(pg_conn)
    passengers = fetch_passengers(pg_conn)
    bookings   = fetch_bookings(pg_conn)
    reviews    = fetch_reviews(pg_conn)
    loyalty    = fetch_loyalty(pg_conn)
    delays     = fetch_delays(pg_conn)
    pg_conn.close()
    log.info("PostgreSQL data loaded — starting migration …")

    # ── Migrate each collection ───────────────────────────────────────────────
    migrate_airports(mongo_db, airports)
    migrate_airlines(mongo_db, airlines, flights, bookings, delays)
    migrate_flights(mongo_db, flights, airlines, airports, aircraft,
                    bookings, reviews, delays)
    migrate_passengers(mongo_db, passengers, loyalty, bookings)
    migrate_bookings(mongo_db, bookings, passengers, flights,
                     airlines, airports, loyalty, reviews)

    # ── Summary ───────────────────────────────────────────────────────────────
    print_summary(mongo_db, start_time)
    mongo_client.close()
    log.info("Migration complete. Log saved to logs/migration.log")


if __name__ == "__main__":
    main()
