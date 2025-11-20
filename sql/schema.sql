-- ===============================================
-- Airbnb Data Warehouse - Star Schema (PRO)
-- sqlite3 compatible
-- ===============================================

-- ----------------------------
-- Drop tables (reset)
-- ----------------------------
DROP TABLE IF EXISTS fact_listings;
DROP TABLE IF EXISTS dim_listing;
DROP TABLE IF EXISTS dim_host;
DROP TABLE IF EXISTS dim_neighborhood;
DROP TABLE IF EXISTS dim_date;

-- ----------------------------
-- Dimension: Date (con surrogate key)
-- ----------------------------
CREATE TABLE dim_date (
    date_sk      INTEGER PRIMARY KEY AUTOINCREMENT,
    date_value   TEXT UNIQUE,  -- 'YYYY-MM-DD'
    year         INTEGER,
    month        INTEGER,
    day          INTEGER,
    week         INTEGER,
    is_weekend   INTEGER
);

-- ----------------------------
-- Dimension: Neighborhood (surrogate key)
-- ----------------------------
CREATE TABLE dim_neighborhood (
    neighborhood_sk    INTEGER PRIMARY KEY AUTOINCREMENT,
    neighbourhood_group TEXT,
    neighbourhood       TEXT,
    UNIQUE (neighbourhood_group, neighbourhood)
);

-- ----------------------------
-- Dimension: Host (SCD2-ready)
-- ----------------------------
CREATE TABLE dim_host (
    host_sk      INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id      INTEGER,          -- business key
    host_name    TEXT,
    calculated_host_listings_count INTEGER,

    effective_from TEXT,           -- 'YYYY-MM-DD'
    effective_to   TEXT,           -- 'YYYY-MM-DD'
    is_current     INTEGER,        -- 1 = vigente, 0 = histórico

    UNIQUE (host_id, effective_from)
);

-- ----------------------------
-- Dimension: Listing (SCD2-ready)
-- ----------------------------
CREATE TABLE dim_listing (
    listing_sk   INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id   INTEGER,          -- business key

    name                 TEXT,
    neighbourhood_group  TEXT,
    neighbourhood        TEXT,
    latitude             REAL,
    longitude            REAL,
    room_type            TEXT,
    price                REAL,
    minimum_nights       INTEGER,
    number_of_reviews    INTEGER,
    reviews_per_month    REAL,
    availability_365     INTEGER,
    license              TEXT,

    effective_from TEXT,           -- 'YYYY-MM-DD'
    effective_to   TEXT,           -- 'YYYY-MM-DD'
    is_current     INTEGER,        -- 1 = vigente

    UNIQUE (listing_id, effective_from)
);

-- ----------------------------
-- Fact Table: fact_listings (surrogate FKs)
-- Grain: 1 fila por listing en el snapshot de carga
-- ----------------------------
CREATE TABLE fact_listings (
    fact_id         INTEGER PRIMARY KEY AUTOINCREMENT,

    listing_sk      INTEGER,
    host_sk         INTEGER,
    neighborhood_sk INTEGER,
    date_sk         INTEGER,   -- fecha de carga (snapshot)

    -- Algunas métricas de conveniencia
    price                REAL,
    number_of_reviews    INTEGER,
    availability_365     INTEGER,

    load_date       TEXT,      -- redundante, pero útil para debugging

    FOREIGN KEY (listing_sk)      REFERENCES dim_listing(listing_sk),
    FOREIGN KEY (host_sk)         REFERENCES dim_host(host_sk),
    FOREIGN KEY (neighborhood_sk) REFERENCES dim_neighborhood(neighborhood_sk),
    FOREIGN KEY (date_sk)         REFERENCES dim_date(date_sk)
);
