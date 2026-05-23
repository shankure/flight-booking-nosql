# Flight Booking System — NoSQL Migration Project

**Course project:** Migration from PostgreSQL to MongoDB  
**Team:** Jovana Filipovska · Darko Koprivnjak

---

## Project overview

A complete ETL pipeline that:
1. Populates a normalised **PostgreSQL** flight-booking database (9 tables, 10 000+ bookings)
2. Migrates and transforms the data into denormalised **MongoDB** documents
3. Validates the migration with automated checks
4. Visualises insights using Plotly in a Jupyter notebook

---

## Prerequisites

| Tool | Version |
|------|---------|
| Docker Desktop | 24+ |
| Docker Compose | v2 (bundled with Docker Desktop) |
| Python | 3.11+ (only needed for local runs outside Docker) |

---

## Quick start (Docker — recommended)

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd flight_booking

# 2. Copy environment file (values already match docker-compose)
cp .env.example .env

# 3. Start PostgreSQL + MongoDB
docker compose up -d postgres mongodb

# 4. Wait ~10 seconds for DBs to be ready, then run each step:

# Step A — generate schema + populate PostgreSQL
docker compose run --rm app python data_gen/generate.py

# Step B — migrate PostgreSQL → MongoDB
docker compose run --rm app python migration/migrate.py

# Step C — validate migration
docker compose run --rm app python validation/validate.py

# Step D — launch Jupyter notebook for visualisations
docker compose run --rm -p 8888:8888 app jupyter notebook \
    --ip=0.0.0.0 --no-browser --allow-root viz/visualisations.ipynb
```

---

## Running locally (without Docker)

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and edit the env file to point at your local DB instances
cp .env.example .env

# Then run the same steps as above, without 'docker compose run --rm app'
python data_gen/generate.py
python migration/migrate.py
python validation/validate.py
jupyter notebook viz/visualisations.ipynb
```

---

## Re-running the migration (idempotency demo)

The migration script uses MongoDB `upsert` operations.  
Running it a second time produces the **same result** — no duplicates, no crashes.

```bash
# Run once
docker compose run --rm app python migration/migrate.py

# Run again — safe
docker compose run --rm app python migration/migrate.py
```

---

## Project structure

```
flight_booking/
├── docker-compose.yml          # PostgreSQL + MongoDB + app service
├── requirements.txt            # Python dependencies
├── .env.example                # Environment variable template
├── db.py                       # Shared DB connection helpers
│
├── sql/                        # PostgreSQL schema
│   └── 01_schema.sql
│
├── data_gen/                   # Data generation
│   └── generate.py             # Faker + OpenFlights → PostgreSQL
│
├── migration/                  # ETL
│   └── migrate.py              # PostgreSQL → MongoDB (with transformations)
│
├── validation/                 # Automated checks
│   └── validate.py             # Record counts, checksums, spot-checks
│
├── viz/                        # Visualisations
│   └── visualisations.ipynb    # Plotly notebook (reads from MongoDB only)
│
└── logs/                       # Migration and validation logs (git-ignored)
```

---

## Dependencies

| Library | Purpose |
|---------|---------|
| `psycopg2-binary` | PostgreSQL driver |
| `pymongo` | MongoDB driver |
| `faker` | Synthetic passenger / booking data |
| `pandas` | Data manipulation during generation |
| `numpy` | Numeric helpers |
| `plotly` | Interactive visualisations |
| `jupyter` | Notebook environment |
| `python-dotenv` | Load `.env` config |
| `tqdm` | Progress bars |
| `colorlog` | Coloured console logging |

---

## Stopping the databases

```bash
docker compose down          # stop containers, keep data volumes
docker compose down -v       # stop containers AND delete all data (full reset)
```
