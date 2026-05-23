"""
data_gen/generate.py
====================
Populates the PostgreSQL flight_booking database with:
  - Real-world data from OpenFlights (airports, airlines, routes)
  - Synthetic data generated with Faker
    (aircraft, passengers, flights, bookings, reviews,
     loyalty_accounts, flight_delays)

Safe to re-run: truncates all tables and re-inserts from scratch.

Usage:
    python data_gen/generate.py
"""

import os
import sys
import random
import string
import logging
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import requests
from faker import Faker
from tqdm import tqdm
import colorlog

# ---------------------------------------------------------------------------
# Make sure the project root is on sys.path so we can import db.py
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from db import get_pg_conn, get_pg_cursor  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
handler = colorlog.StreamHandler()
handler.setFormatter(colorlog.ColoredFormatter(
    "%(log_color)s%(levelname)-8s%(reset)s %(message)s"
))
logging.basicConfig(level=logging.INFO, handlers=[handler])
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RANDOM_SEED        = 42
NUM_AIRCRAFT       = 30
NUM_PASSENGERS     = 3_500
NUM_FLIGHTS        = 2_500
NUM_BOOKINGS       = 12_000   # must be ≥ 10 000
REVIEW_RATE        = 0.55     # fraction of completed bookings that get a review
DELAY_RATE         = 0.25     # fraction of flights that have a delay record

CABIN_CLASSES      = ["economy", "business", "first"]
CABIN_WEIGHTS      = [0.75, 0.20, 0.05]
CABIN_MULTIPLIERS  = {"economy": 1.0, "business": 2.5, "first": 4.5}

BOOKING_STATUSES   = ["confirmed", "cancelled", "completed", "no_show"]
BOOKING_WEIGHTS    = [0.30, 0.10, 0.55, 0.05]

FLIGHT_STATUSES    = ["scheduled", "delayed", "cancelled", "completed"]
FLIGHT_WEIGHTS     = [0.25, 0.10, 0.05, 0.60]

DELAY_REASONS      = ["weather", "technical", "crew", "air_traffic", "security", "other"]
LOYALTY_TIERS      = ["bronze", "silver", "gold", "platinum"]

AIRCRAFT_MODELS = [
    ("Boeing",   "737-800",  189, 5765),
    ("Boeing",   "737 MAX 8",178, 6570),
    ("Boeing",   "777-300ER",396, 13650),
    ("Boeing",   "787-9",    296, 14140),
    ("Boeing",   "747-8",    467, 14815),
    ("Airbus",   "A320-200", 180, 6150),
    ("Airbus",   "A321neo",  194, 7400),
    ("Airbus",   "A330-300", 277, 11750),
    ("Airbus",   "A350-900", 325, 15000),
    ("Airbus",   "A380-800", 555, 15200),
    ("Embraer",  "E190",      98, 4537),
    ("Embraer",  "E195-E2",  146, 4800),
    ("Bombardier","CRJ-900",  90, 2876),
]

OPENFLIGHTS_URLS = {
    "airports": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat",
    "airlines": "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airlines.dat",
    "routes":   "https://raw.githubusercontent.com/jpatokal/openflights/master/data/routes.dat",
}

fake  = Faker()
rng   = np.random.default_rng(RANDOM_SEED)
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)


# ===========================================================================
# Helpers
# ===========================================================================

def random_booking_reference(existing: set) -> str:
    """Generate a unique 6-character alphanumeric booking reference."""
    while True:
        ref = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if ref not in existing:
            existing.add(ref)
            return ref


def random_seat(total_seats: int, taken: set) -> str:
    """Generate a seat like 14B that hasn't been taken on this flight."""
    rows   = max(1, total_seats // 6)
    cols   = list("ABCDEF")
    attempts = 0
    while attempts < 500:
        row  = random.randint(1, rows)
        col  = random.choice(cols)
        seat = f"{row}{col}"
        if seat not in taken:
            taken.add(seat)
            return seat
        attempts += 1
    # Fallback: just pick any seat label (very unlikely to collide)
    return f"{random.randint(1,99)}{random.choice(cols)}"


def truncate_all(conn) -> None:
    """Truncate every table in dependency order (children first)."""
    tables = [
        "flight_delays", "loyalty_accounts", "reviews",
        "bookings", "passengers", "flights",
        "aircraft", "routes", "airlines", "airports",
    ]
    with get_pg_cursor(conn) as cur:
        cur.execute(f"TRUNCATE {', '.join(tables)} RESTART IDENTITY CASCADE;")
    conn.commit()
    log.info("All tables truncated")


# ===========================================================================
# OpenFlights loaders
# ===========================================================================

def load_airports(conn) -> dict:
    """
    Download airports.dat, filter to active airports with valid IATA codes
    and coordinates, insert into DB.  Returns {iata_code: airport_id}.
    """
    log.info("Downloading airports from OpenFlights …")
    cols = [
        "of_id", "name", "city", "country", "iata_code", "icao_code",
        "latitude", "longitude", "altitude", "timezone_offset",
        "dst", "timezone", "type", "source",
    ]
    df = pd.read_csv(OPENFLIGHTS_URLS["airports"], header=None,
                     names=cols, na_values=["\\N", ""])

    # Keep only proper airports with a 3-letter IATA code
    df = df[
        (df["iata_code"].notna()) &
        (df["iata_code"].str.len() == 3) &
        (df["latitude"].notna()) &
        (df["longitude"].notna()) &
        (df["type"] == "airport")
    ].copy()

    # Fill missing string fields
    df["timezone"] = df["timezone"].fillna("UTC")
    df["city"]     = df["city"].fillna("Unknown")
    df["name"]     = df["name"].fillna("Unknown Airport")
    df["country"]  = df["country"].fillna("Unknown")

    # Deduplicate on iata_code (keep first occurrence)
    df = df.drop_duplicates(subset="iata_code")

    # Sample to keep dataset manageable
    df = df.sample(n=min(1_800, len(df)), random_state=RANDOM_SEED)

    rows = [
        (row.iata_code.strip(), row.name[:200], row.city[:100],
         row.country[:100], float(row.latitude), float(row.longitude),
         str(row.timezone)[:50])
        for row in df.itertuples()
    ]

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO airports
               (iata_code, name, city, country, latitude, longitude, timezone)
               VALUES (%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT (iata_code) DO NOTHING""",
            rows,
        )
    conn.commit()

    # Build lookup dict
    with get_pg_cursor(conn) as cur:
        cur.execute("SELECT airport_id, iata_code FROM airports")
        mapping = {r["iata_code"]: r["airport_id"] for r in cur.fetchall()}

    log.info(f"  Inserted {len(mapping):,} airports")
    return mapping


def load_airlines(conn) -> dict:
    """
    Download airlines.dat, filter active airlines with IATA codes.
    Returns {iata_code: airline_id}.
    """
    log.info("Downloading airlines from OpenFlights …")
    cols = [
        "of_id", "name", "alias", "iata_code", "icao_code",
        "callsign", "country", "active",
    ]
    df = pd.read_csv(OPENFLIGHTS_URLS["airlines"], header=None,
                     names=cols, na_values=["\\N", ""])

    df = df[
        (df["iata_code"].notna()) &
        (df["iata_code"].str.len() == 2) &
        (df["active"] == "Y") &
        (df["country"].notna())
    ].copy()

    df = df.drop_duplicates(subset="iata_code")

    rows = [
        (row.iata_code.strip(), row.name[:200], row.country[:100], True)
        for row in df.itertuples()
    ]

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO airlines (iata_code, name, country, active)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (iata_code) DO NOTHING""",
            rows,
        )
    conn.commit()

    with get_pg_cursor(conn) as cur:
        cur.execute("SELECT airline_id, iata_code FROM airlines")
        mapping = {r["iata_code"]: r["airline_id"] for r in cur.fetchall()}

    log.info(f"  Inserted {len(mapping):,} airlines")
    return mapping


def load_routes(conn, airport_map: dict, airline_map: dict) -> None:
    """
    Download routes.dat, compute Haversine distance, insert into routes table.
    Only keeps routes where both airports and the airline exist in our DB.
    """
    log.info("Downloading routes from OpenFlights …")
    cols = [
        "airline_iata", "airline_of_id",
        "src_iata", "src_of_id",
        "dst_iata", "dst_of_id",
        "codeshare", "stops", "equipment",
    ]
    df = pd.read_csv(OPENFLIGHTS_URLS["routes"], header=None,
                     names=cols, na_values=["\\N", ""])

    df = df[
        (df["airline_iata"].notna()) &
        (df["src_iata"].notna()) &
        (df["dst_iata"].notna()) &
        (df["stops"] == 0)          # direct flights only
    ].copy()

    # Filter to rows where we have all three IDs
    df = df[
        df["airline_iata"].isin(airline_map) &
        df["src_iata"].isin(airport_map) &
        df["dst_iata"].isin(airport_map)
    ].copy()

    df = df.drop_duplicates(subset=["airline_iata", "src_iata", "dst_iata"])

    # Retrieve lat/lon for distance calculation
    with get_pg_cursor(conn) as cur:
        cur.execute("SELECT iata_code, latitude, longitude FROM airports")
        coords = {r["iata_code"]: (float(r["latitude"]), float(r["longitude"]))
                  for r in cur.fetchall()}

    def haversine(iata1: str, iata2: str) -> int:
        lat1, lon1 = map(np.radians, coords[iata1])
        lat2, lon2 = map(np.radians, coords[iata2])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = np.sin(dlat/2)**2 + np.cos(lat1)*np.cos(lat2)*np.sin(dlon/2)**2
        return max(1, int(6371 * 2 * np.arcsin(np.sqrt(a))))

    rows = []
    for r in df.itertuples():
        src, dst = r.src_iata.strip(), r.dst_iata.strip()
        if src == dst:
            continue
        dist = haversine(src, dst)
        rows.append((
            airline_map[r.airline_iata.strip()],
            airport_map[src],
            airport_map[dst],
            dist,
        ))

    # Sample to keep it manageable
    random.shuffle(rows)
    rows = rows[:5_000]

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO routes
               (airline_id, origin_id, destination_id, distance_km)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT (airline_id, origin_id, destination_id) DO NOTHING""",
            rows,
        )
    conn.commit()

    with get_pg_cursor(conn) as cur:
        cur.execute("SELECT COUNT(*) AS n FROM routes")
        count = cur.fetchone()["n"]

    log.info(f"  Inserted {count:,} routes")


# ===========================================================================
# Synthetic data generators
# ===========================================================================

def generate_aircraft(conn) -> list:
    """Insert aircraft models and return list of aircraft_ids."""
    log.info("Generating aircraft …")
    rows = []
    for manufacturer, model, seats, range_km in AIRCRAFT_MODELS:
        # Add a few variants with ±10 % seat variation
        for _ in range(random.randint(1, 3)):
            seat_var = seats + random.randint(-10, 10)
            rows.append((model, manufacturer, max(50, seat_var), range_km))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO aircraft (model, manufacturer, total_seats, range_km)
               VALUES (%s,%s,%s,%s)""",
            rows,
        )
    conn.commit()

    with get_pg_cursor(conn) as cur:
        cur.execute("SELECT aircraft_id, total_seats FROM aircraft")
        result = cur.fetchall()

    log.info(f"  Inserted {len(result):,} aircraft")
    return result   # list of RealDictRow with aircraft_id, total_seats


def generate_passengers(conn) -> list:
    """Generate synthetic passengers. Returns list of passenger_ids."""
    log.info(f"Generating {NUM_PASSENGERS:,} passengers …")
    rows = []
    emails_seen = set()

    for _ in range(NUM_PASSENGERS):
        while True:
            email = fake.unique.email()
            if email not in emails_seen:
                emails_seen.add(email)
                break
        dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
        rows.append((
            fake.first_name(),
            fake.last_name(),
            email,
            fake.phone_number()[:20],
            dob,
            fake.country(),
            fake.bothify(text="??#######").upper(),   # passport-like
            fake.date_time_between(start_date="-5y", end_date="now"),
        ))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO passengers
               (first_name, last_name, email, phone,
                date_of_birth, nationality, passport_number, created_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            rows,
        )
    conn.commit()

    with get_pg_cursor(conn) as cur:
        cur.execute("SELECT passenger_id FROM passengers")
        ids = [r["passenger_id"] for r in cur.fetchall()]

    log.info(f"  Inserted {len(ids):,} passengers")
    return ids


def generate_flights(conn, airline_map: dict, aircraft_rows: list,
                     airport_map: dict) -> list:
    """
    Generate synthetic flights using real route pairs where possible.
    Returns list of dicts {flight_id, total_seats, base_price, status,
                           airline_id, origin_iata, destination_iata}.
    """
    log.info(f"Generating {NUM_FLIGHTS:,} flights …")

    # Pull available routes to reuse real airport pairs
    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT r.airline_id, r.origin_id, r.destination_id, r.distance_km,
                   ao.iata_code AS origin_iata, ad.iata_code AS dest_iata
            FROM   routes r
            JOIN   airports ao ON ao.airport_id = r.origin_id
            JOIN   airports ad ON ad.airport_id = r.destination_id
        """)
        route_pool = cur.fetchall()

    airline_ids  = list(airline_map.values())
    airport_ids  = list(airport_map.values())
    aircraft_ids = [r["aircraft_id"] for r in aircraft_rows]
    seats_map    = {r["aircraft_id"]: r["total_seats"] for r in aircraft_rows}

    start_window = datetime.now() - timedelta(days=365)
    rows = []

    for i in range(NUM_FLIGHTS):
        # 70 % of flights follow a real route, 30 % are fully synthetic
        if route_pool and random.random() < 0.70:
            route    = random.choice(route_pool)
            al_id    = route["airline_id"]
            orig_id  = route["origin_id"]
            dest_id  = route["destination_id"]
            dist_km  = route["distance_km"]
        else:
            al_id   = random.choice(airline_ids)
            orig_id = random.choice(airport_ids)
            dest_id = random.choice(airport_ids)
            while dest_id == orig_id:
                dest_id = random.choice(airport_ids)
            dist_km = random.randint(300, 12_000)

        ac_id        = random.choice(aircraft_ids)
        total_seats  = seats_map[ac_id]
        base_price   = round(max(49, dist_km * random.uniform(0.05, 0.18)), 2)
        flight_num   = f"{random.choice(['AA','BA','LH','AF','EK','TK','QR'])}{random.randint(100,9999)}"

        dep  = start_window + timedelta(
            days=random.randint(0, 730),
            hours=random.randint(0, 23),
            minutes=random.choice([0, 15, 30, 45]),
        )
        # Flight duration: ~100 km/h groundspeed on average
        duration_hrs = max(0.5, dist_km / 800)
        arr = dep + timedelta(hours=duration_hrs)

        status = random.choices(FLIGHT_STATUSES, weights=FLIGHT_WEIGHTS)[0]

        rows.append((
            flight_num, al_id, ac_id, orig_id, dest_id,
            dep, arr, base_price, total_seats, status,
        ))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO flights
               (flight_number, airline_id, aircraft_id, origin_id, destination_id,
                scheduled_departure, scheduled_arrival, base_price, total_seats, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            rows,
        )
    conn.commit()

    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT f.flight_id, f.total_seats, f.base_price, f.status,
                   f.airline_id,
                   ao.iata_code AS origin_iata,
                   ad.iata_code AS dest_iata
            FROM   flights f
            JOIN   airports ao ON ao.airport_id = f.origin_id
            JOIN   airports ad ON ad.airport_id = f.destination_id
        """)
        result = cur.fetchall()

    log.info(f"  Inserted {len(result):,} flights")
    return result


def generate_bookings(conn, passenger_ids: list, flight_rows: list) -> list:
    """
    Generate bookings. Returns list of dicts
    {booking_id, flight_id, passenger_id, status, price_paid}.
    """
    log.info(f"Generating {NUM_BOOKINGS:,} bookings …")

    references_seen: set = set()
    # Track taken seats per flight
    seats_taken: dict = {}

    rows = []
    for _ in tqdm(range(NUM_BOOKINGS), desc="  bookings", unit="rec"):
        flight      = random.choice(flight_rows)
        fid         = flight["flight_id"]
        total_seats = flight["total_seats"]

        if fid not in seats_taken:
            seats_taken[fid] = set()

        cabin       = random.choices(CABIN_CLASSES, weights=CABIN_WEIGHTS)[0]
        multiplier  = CABIN_MULTIPLIERS[cabin]
        price_paid  = round(float(flight["base_price"]) * multiplier
                            * random.uniform(0.85, 1.20), 2)
        status      = random.choices(BOOKING_STATUSES, weights=BOOKING_WEIGHTS)[0]

        # Align booking status with flight status
        if flight["status"] == "cancelled":
            status = "cancelled"
        elif flight["status"] == "completed":
            status = random.choices(
                ["completed", "no_show"], weights=[0.92, 0.08]
            )[0]

        ref    = random_booking_reference(references_seen)
        seat   = random_seat(total_seats, seats_taken[fid])
        p_id   = random.choice(passenger_ids)
        b_date = fake.date_time_between(start_date="-2y", end_date="now")

        rows.append((ref, p_id, fid, seat, cabin, price_paid, b_date, status))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO bookings
               (booking_reference, passenger_id, flight_id, seat_number,
                cabin_class, price_paid, booking_date, status)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            rows,
        )
    conn.commit()

    with get_pg_cursor(conn) as cur:
        cur.execute("""
            SELECT booking_id, flight_id, passenger_id, status, price_paid
            FROM   bookings
        """)
        result = cur.fetchall()

    log.info(f"  Inserted {len(result):,} bookings")
    return result


def generate_reviews(conn, booking_rows: list) -> None:
    """Generate reviews for a fraction of completed bookings."""
    log.info("Generating reviews …")

    completed = [b for b in booking_rows if b["status"] == "completed"]
    sample    = random.sample(completed, int(len(completed) * REVIEW_RATE))

    sentiments = {
        5: ["Excellent flight!", "Fantastic experience", "Highly recommend"],
        4: ["Good flight overall", "Pleasant journey", "Would fly again"],
        3: ["Decent but nothing special", "Average experience", "It was okay"],
        2: ["Disappointing", "Below expectations", "Several issues"],
        1: ["Terrible experience", "Would not recommend", "Very poor service"],
    }

    rows = []
    for b in sample:
        rating = int(rng.choice([1,2,3,4,5], p=[0.05,0.08,0.17,0.35,0.35]))
        title  = random.choice(sentiments[rating])
        body   = fake.paragraph(nb_sentences=random.randint(2, 5))
        ts     = fake.date_time_between(start_date="-1y", end_date="now")
        rows.append((b["booking_id"], rating, title, body, ts))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO reviews
               (booking_id, rating, title, body, created_at)
               VALUES (%s,%s,%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            rows,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows):,} reviews")


def generate_loyalty_accounts(conn, passenger_ids: list,
                               booking_rows: list) -> None:
    """Create a loyalty account for every passenger, deriving stats from bookings."""
    log.info("Generating loyalty accounts …")

    # Aggregate bookings per passenger
    from collections import defaultdict
    stats: dict = defaultdict(lambda: {"flights": 0, "spent": 0.0})
    for b in booking_rows:
        if b["status"] in ("confirmed", "completed"):
            stats[b["passenger_id"]]["flights"] += 1
            stats[b["passenger_id"]]["spent"]   += float(b["price_paid"])

    def tier_from_flights(n: int) -> str:
        if n >= 50:  return "platinum"
        if n >= 20:  return "gold"
        if n >= 5:   return "silver"
        return "bronze"

    rows = []
    for pid in passenger_ids:
        s       = stats[pid]
        flights = s["flights"]
        spent   = round(s["spent"], 2)
        points  = flights * 100 + int(spent // 10)
        tier    = tier_from_flights(flights)
        joined  = fake.date_time_between(start_date="-5y", end_date="-1y")
        rows.append((pid, tier, points, flights, spent, joined))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO loyalty_accounts
               (passenger_id, tier, points, total_flights, total_spent, joined_at)
               VALUES (%s,%s,%s,%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            rows,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows):,} loyalty accounts")


def generate_flight_delays(conn, flight_rows: list) -> None:
    """Generate delay records for a fraction of non-cancelled flights."""
    log.info("Generating flight delays …")

    eligible = [f for f in flight_rows if f["status"] != "cancelled"]
    sample   = random.sample(eligible, int(len(eligible) * DELAY_RATE))

    rows = []
    for f in sample:
        delay_min = int(rng.integers(5, 300))
        reason    = random.choice(DELAY_REASONS)
        reported  = fake.date_time_between(start_date="-1y", end_date="now")
        rows.append((f["flight_id"], delay_min, reason, reported))

    with get_pg_cursor(conn) as cur:
        cur.executemany(
            """INSERT INTO flight_delays
               (flight_id, delay_minutes, reason, reported_at)
               VALUES (%s,%s,%s,%s)
               ON CONFLICT DO NOTHING""",
            rows,
        )
    conn.commit()
    log.info(f"  Inserted {len(rows):,} flight delay records")


# ===========================================================================
# Summary
# ===========================================================================

def print_summary(conn) -> None:
    tables = [
        "airports", "airlines", "routes", "aircraft",
        "passengers", "flights", "bookings",
        "reviews", "loyalty_accounts", "flight_delays",
    ]
    log.info("=" * 50)
    log.info("DATABASE POPULATION SUMMARY")
    log.info("=" * 50)
    with get_pg_cursor(conn) as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) AS n FROM {t}")
            n = cur.fetchone()["n"]
            status = "✓" if (t != "bookings" or n >= 10_000) else "✗ BELOW 10k!"
            log.info(f"  {t:<25} {n:>8,} rows  {status}")
    log.info("=" * 50)


# ===========================================================================
# Main
# ===========================================================================

def main() -> None:
    log.info("Connecting to PostgreSQL …")
    conn = get_pg_conn()

    log.info("Truncating all tables for a clean run …")
    truncate_all(conn)

    # ── Real-world data ──────────────────────────────────────────────────────
    airport_map = load_airports(conn)
    airline_map = load_airlines(conn)
    load_routes(conn, airport_map, airline_map)

    # ── Synthetic data ───────────────────────────────────────────────────────
    aircraft_rows  = generate_aircraft(conn)
    passenger_ids  = generate_passengers(conn)
    flight_rows    = generate_flights(conn, airline_map, aircraft_rows, airport_map)
    booking_rows   = generate_bookings(conn, passenger_ids, flight_rows)
    generate_reviews(conn, booking_rows)
    generate_loyalty_accounts(conn, passenger_ids, booking_rows)
    generate_flight_delays(conn, flight_rows)

    print_summary(conn)
    conn.close()
    log.info("Done! Database is ready.")


if __name__ == "__main__":
    main()
