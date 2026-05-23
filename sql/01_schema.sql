-- =============================================================================
-- Flight Booking System — PostgreSQL Schema
-- =============================================================================

-- Clean slate (safe to re-run)
DROP TABLE IF EXISTS flight_delays      CASCADE;
DROP TABLE IF EXISTS loyalty_accounts   CASCADE;
DROP TABLE IF EXISTS reviews            CASCADE;
DROP TABLE IF EXISTS bookings           CASCADE;
DROP TABLE IF EXISTS passengers         CASCADE;
DROP TABLE IF EXISTS flights            CASCADE;
DROP TABLE IF EXISTS aircraft           CASCADE;
DROP TABLE IF EXISTS routes             CASCADE;
DROP TABLE IF EXISTS airlines           CASCADE;
DROP TABLE IF EXISTS airports           CASCADE;

-- =============================================================================
-- 1. airports
-- =============================================================================
CREATE TABLE airports (
    airport_id      SERIAL          PRIMARY KEY,
    iata_code       CHAR(3)         NOT NULL UNIQUE,
    name            VARCHAR(200)    NOT NULL,
    city            VARCHAR(100)    NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    latitude        NUMERIC(9,6)    NOT NULL,
    longitude       NUMERIC(9,6)    NOT NULL,
    timezone        VARCHAR(50)     NOT NULL,
    CONSTRAINT chk_latitude  CHECK (latitude  BETWEEN -90  AND  90),
    CONSTRAINT chk_longitude CHECK (longitude BETWEEN -180 AND 180)
);

-- =============================================================================
-- 2. airlines
-- =============================================================================
CREATE TABLE airlines (
    airline_id      SERIAL          PRIMARY KEY,
    iata_code       CHAR(2)         NOT NULL UNIQUE,
    name            VARCHAR(200)    NOT NULL,
    country         VARCHAR(100)    NOT NULL,
    active          BOOLEAN         NOT NULL DEFAULT TRUE
);

-- =============================================================================
-- 3. aircraft
-- =============================================================================
CREATE TABLE aircraft (
    aircraft_id     SERIAL          PRIMARY KEY,
    model           VARCHAR(100)    NOT NULL,
    manufacturer    VARCHAR(100)    NOT NULL,
    total_seats     SMALLINT        NOT NULL,
    range_km        INT             NOT NULL,
    CONSTRAINT chk_seats    CHECK (total_seats > 0),
    CONSTRAINT chk_range_km CHECK (range_km    > 0)
);

-- =============================================================================
-- 4. routes  (airport pairs served by an airline)
-- =============================================================================
CREATE TABLE routes (
    route_id        SERIAL          PRIMARY KEY,
    airline_id      INT             NOT NULL REFERENCES airlines(airline_id),
    origin_id       INT             NOT NULL REFERENCES airports(airport_id),
    destination_id  INT             NOT NULL REFERENCES airports(airport_id),
    distance_km     INT             NOT NULL,
    CONSTRAINT chk_distance     CHECK (distance_km > 0),
    CONSTRAINT chk_no_self_loop CHECK (origin_id <> destination_id),
    CONSTRAINT uq_route         UNIQUE (airline_id, origin_id, destination_id)
);

-- =============================================================================
-- 5. flights
-- =============================================================================
CREATE TABLE flights (
    flight_id           SERIAL          PRIMARY KEY,
    flight_number       VARCHAR(10)     NOT NULL,
    airline_id          INT             NOT NULL REFERENCES airlines(airline_id),
    aircraft_id         INT             NOT NULL REFERENCES aircraft(aircraft_id),
    origin_id           INT             NOT NULL REFERENCES airports(airport_id),
    destination_id      INT             NOT NULL REFERENCES airports(airport_id),
    scheduled_departure TIMESTAMP       NOT NULL,
    scheduled_arrival   TIMESTAMP       NOT NULL,
    base_price          NUMERIC(10,2)   NOT NULL,
    total_seats         SMALLINT        NOT NULL,
    status              VARCHAR(20)     NOT NULL DEFAULT 'scheduled',
    CONSTRAINT chk_arrival_after_departure CHECK (scheduled_arrival > scheduled_departure),
    CONSTRAINT chk_base_price              CHECK (base_price >= 0),
    CONSTRAINT chk_flight_status           CHECK (status IN ('scheduled','delayed','cancelled','completed')),
    CONSTRAINT chk_flight_seats            CHECK (total_seats > 0)
);

-- =============================================================================
-- 6. passengers
-- =============================================================================
CREATE TABLE passengers (
    passenger_id    SERIAL          PRIMARY KEY,
    first_name      VARCHAR(100)    NOT NULL,
    last_name       VARCHAR(100)    NOT NULL,
    email           VARCHAR(200)    NOT NULL UNIQUE,
    phone           VARCHAR(20),
    date_of_birth   DATE            NOT NULL,
    nationality     VARCHAR(100)    NOT NULL,
    passport_number VARCHAR(20)     NOT NULL UNIQUE,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_dob CHECK (date_of_birth < CURRENT_DATE)
);

-- =============================================================================
-- 7. bookings  (≥10 000 rows required)
-- =============================================================================
CREATE TABLE bookings (
    booking_id          SERIAL          PRIMARY KEY,
    booking_reference   CHAR(6)         NOT NULL UNIQUE,
    passenger_id        INT             NOT NULL REFERENCES passengers(passenger_id),
    flight_id           INT             NOT NULL REFERENCES flights(flight_id),
    seat_number         VARCHAR(4)      NOT NULL,
    cabin_class         VARCHAR(10)     NOT NULL,
    price_paid          NUMERIC(10,2)   NOT NULL,
    booking_date        TIMESTAMP       NOT NULL DEFAULT NOW(),
    status              VARCHAR(20)     NOT NULL DEFAULT 'confirmed',
    CONSTRAINT chk_cabin_class    CHECK (cabin_class IN ('economy','business','first')),
    CONSTRAINT chk_booking_status CHECK (status IN ('confirmed','cancelled','completed','no_show')),
    CONSTRAINT chk_price_paid     CHECK (price_paid >= 0),
    CONSTRAINT uq_flight_seat     UNIQUE (flight_id, seat_number)
);

-- =============================================================================
-- 8. reviews
-- =============================================================================
CREATE TABLE reviews (
    review_id       SERIAL          PRIMARY KEY,
    booking_id      INT             NOT NULL UNIQUE REFERENCES bookings(booking_id),
    rating          SMALLINT        NOT NULL,
    title           VARCHAR(200),
    body            TEXT,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_rating CHECK (rating BETWEEN 1 AND 5)
);

-- =============================================================================
-- 9. loyalty_accounts
-- =============================================================================
CREATE TABLE loyalty_accounts (
    loyalty_id      SERIAL          PRIMARY KEY,
    passenger_id    INT             NOT NULL UNIQUE REFERENCES passengers(passenger_id),
    tier            VARCHAR(10)     NOT NULL DEFAULT 'bronze',
    points          INT             NOT NULL DEFAULT 0,
    total_flights   INT             NOT NULL DEFAULT 0,
    total_spent     NUMERIC(12,2)   NOT NULL DEFAULT 0,
    joined_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_tier         CHECK (tier IN ('bronze','silver','gold','platinum')),
    CONSTRAINT chk_points       CHECK (points >= 0),
    CONSTRAINT chk_total_flights CHECK (total_flights >= 0),
    CONSTRAINT chk_total_spent  CHECK (total_spent >= 0)
);

-- =============================================================================
-- 10. flight_delays
-- =============================================================================
CREATE TABLE flight_delays (
    delay_id        SERIAL          PRIMARY KEY,
    flight_id       INT             NOT NULL UNIQUE REFERENCES flights(flight_id),
    delay_minutes   INT             NOT NULL DEFAULT 0,
    reason          VARCHAR(50)     NOT NULL,
    reported_at     TIMESTAMP       NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_delay_minutes CHECK (delay_minutes >= 0),
    CONSTRAINT chk_delay_reason  CHECK (reason IN (
        'weather','technical','crew','air_traffic','security','other'
    ))
);

-- =============================================================================
-- Indexes (for join performance during migration)
-- =============================================================================
CREATE INDEX idx_flights_airline    ON flights(airline_id);
CREATE INDEX idx_flights_origin     ON flights(origin_id);
CREATE INDEX idx_flights_dest       ON flights(destination_id);
CREATE INDEX idx_bookings_passenger ON bookings(passenger_id);
CREATE INDEX idx_bookings_flight    ON bookings(flight_id);
CREATE INDEX idx_bookings_status    ON bookings(status);
CREATE INDEX idx_reviews_booking    ON reviews(booking_id);
CREATE INDEX idx_loyalty_passenger  ON loyalty_accounts(passenger_id);
CREATE INDEX idx_delays_flight      ON flight_delays(flight_id);
