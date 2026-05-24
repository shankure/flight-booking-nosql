# Flight Booking System — NoSQL Migration Project

**Course:** NoSQL Databases  
**Team:** Jovana Filipovska · Darko Koprivnjak  
**Stack:** PostgreSQL 15 · MongoDB 7 · Python 3.12 · Docker Compose · Plotly · Jupyter

---

## Project Overview

A complete ETL pipeline that migrates a normalised **PostgreSQL** flight-booking database
(10 tables, 12 000+ bookings) into a denormalised **MongoDB** document store, validates
the result with automated checks, and visualises insights through an interactive Jupyter
notebook.

```
PostgreSQL  ──►  migrate.py  ──►  MongoDB  ──►  visualisations.ipynb
     │                                │
     └──────────  validate.py  ───────┘
                  (13 checks, all PASS)
```

---

## Quick Start (Docker — recommended)

### Prerequisites
| Tool | Version |
|------|---------|
| Docker Desktop | 24 + |
| Python | 3.12 |

### 1 — Clone and configure

```bash
git clone <your-repo-url>
cd flight_booking
cp .env.example .env
```

> **.env is pre-configured to match docker-compose.yml — no edits needed.**

### 2 — Start the databases

```bash
docker compose up -d postgres mongodb
```

Wait ~15 seconds, then verify both containers are healthy:

```bash
docker compose ps
```

Both `flight_postgres` and `flight_mongo` should show **healthy**.

### 3 — Fix PostgreSQL network auth (one-time, required on first run)

```bash
docker compose exec postgres sh -c "echo 'host all all 172.0.0.0/8 trust' >> /var/lib/postgresql/data/pg_hba.conf"
docker compose exec postgres psql -U flight_user -d flight_booking -c "SELECT pg_reload_conf();"
```

### 4 — Apply the schema

```bash
docker compose exec postgres psql -U flight_user -d flight_booking -f /docker-entrypoint-initdb.d/01_schema.sql
```

### 5 — Activate the Python environment

```bash
# Create (first time only)
python -m venv .venv

# Activate — Windows
.venv\Scripts\activate

# Activate — Mac/Linux
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
pip install statsmodels   # required for trendline in visualization 5
```

---

## Running the Pipeline

Run each step in order. All commands assume the virtual environment is active.

### Step A — Populate PostgreSQL

Downloads real airport/airline/route data from OpenFlights and generates synthetic
passengers, flights, bookings, reviews, loyalty accounts, and flight delays.

```bash
python data_gen/generate.py
```

Expected output (takes ~60 seconds):
```
airports                  1,800 rows  ✓
airlines                    990 rows  ✓
routes                    5,000 rows  ✓
aircraft                     29 rows  ✓
passengers                3,500 rows  ✓
flights                   2,500 rows  ✓
bookings                 12,000 rows  ✓
reviews                   4,918 rows  ✓
loyalty_accounts          3,500 rows  ✓
flight_delays               595 rows  ✓
Done! Database is ready.
```

### Step B — Run the migration

Migrates all data from PostgreSQL to MongoDB with 7 derived fields computed.

```bash
python migration/migrate.py
```

Expected output:
```
airports               1,800 documents
airlines                 990 documents
flights                2,500 documents
passengers             3,500 documents
bookings              12,000 documents
Migration complete. Log saved to logs/migration.log
```

### Step C — Verify idempotency (presentation demo)

Run the migration a **second time** — counts must be identical, no duplicates:

```bash
python migration/migrate.py
```

Second run will show `inserted=0, updated=XXXX` for every collection — proving idempotency.

### Step D — Run validation

```bash
python validation/validate.py
```

Expected output:
```
Total checks : 13
Passed       : 13
Failed       : 0
✓ All checks passed — migration validated successfully.
Full report saved to logs/validation.log
```

### Step E — Launch visualisations

```bash
jupyter notebook viz/visualisations.ipynb
```

Your browser opens automatically. Run **Kernel → Restart & Run All** to render all 5 charts.

---

## Project Structure

```
flight_booking/
├── docker-compose.yml          # PostgreSQL (port 5433) + MongoDB (port 27018)
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
├── .env                        # Local config (git-ignored)
├── db.py                       # Shared DB connection helpers
├── README.md                   # This file
│
├── sql/
│   └── 01_schema.sql           # 10-table PostgreSQL schema with all constraints
│
├── data_gen/
│   └── generate.py             # OpenFlights + Faker → PostgreSQL
│
├── migration/
│   └── migrate.py              # PostgreSQL → MongoDB (idempotent, 7 derived fields)
│
├── validation/
│   └── validate.py             # 13 automated checks, PASS/FAIL report
│
├── viz/
│   └── visualisations.ipynb    # 5 Plotly charts reading from MongoDB only
│
└── logs/                       # Migration and validation logs (git-ignored)
    ├── migration.log
    └── validation.log
```

---

## Port Configuration

Both databases use non-default ports to avoid conflicts with locally installed instances.

| Service    | Host port | Container port |
|------------|-----------|----------------|
| PostgreSQL | **5433**  | 5432           |
| MongoDB    | **27018** | 27017          |

The `.env` file and `docker-compose.yml` are already configured with these values.

---

## Derived Fields Computed During Migration

| Field | Collection | Description |
|-------|-----------|-------------|
| `loyalty_tier` | bookings, passengers | bronze / silver / gold / platinum from total_flights |
| `total_spent` | passengers | SUM of price_paid per passenger |
| `occupancy_rate` | flights | active bookings / total seats × 100 |
| `revenue_per_km` | flights | total revenue / route distance |
| `avg_review_score` | flights | mean review rating per flight |
| `on_time_rate` | airlines | % of flights with no delay record |
| `price_category` | flights | budget / standard / premium from base_price |

---

## Dependencies

| Library | Version | Purpose |
|---------|---------|---------|
| `psycopg2-binary` | 2.9.12 | PostgreSQL driver |
| `pymongo` | 4.8.0 | MongoDB driver |
| `faker` | 25.9.2 | Synthetic data generation |
| `pandas` | 2.2.3 | Data manipulation |
| `numpy` | 2.0.2 | Numeric helpers |
| `plotly` | 5.22.0 | Interactive visualisations |
| `jupyter` | 1.0.0 | Notebook environment |
| `python-dotenv` | 1.0.1 | Load .env config |
| `tqdm` | 4.66.4 | Progress bars |
| `colorlog` | 6.8.2 | Coloured console logging |
| `statsmodels` | latest | Trendline in visualization 5 |

---

## Stopping the Databases

```bash
# Stop containers, keep data
docker compose down

# Stop containers AND delete all data (full reset)
docker compose down -v
```

> After `down -v`, repeat Steps 3 and 4 (pg_hba fix + schema) before running the pipeline again.
