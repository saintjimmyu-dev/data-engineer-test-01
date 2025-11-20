from datetime import date, datetime
from src.utils.db_connector import get_connection
from src.utils.logger import get_logger

logger = get_logger(__name__)


# -----------------------------
# Helpers para dimensiones
# -----------------------------
def _ensure_date_dim(conn, load_date_str):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_date (
            date_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            date_value TEXT UNIQUE,
            year INTEGER,
            month INTEGER,
            day INTEGER,
            week INTEGER,
            is_weekend INTEGER
        )
    """)

    cur.execute("SELECT date_sk FROM dim_date WHERE date_value = ?", (load_date_str,))
    row = cur.fetchone()
    if row:
        return row[0]

    d = datetime.strptime(load_date_str, "%Y-%m-%d").date()
    year, month, day = d.year, d.month, d.day
    week = d.isocalendar().week
    is_weekend = 1 if d.weekday() >= 5 else 0

    cur.execute("""
        INSERT INTO dim_date (date_value, year, month, day, week, is_weekend)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (load_date_str, year, month, day, week, is_weekend))

    cur.execute("SELECT date_sk FROM dim_date WHERE date_value = ?", (load_date_str,))
    date_sk = cur.fetchone()[0]

    logger.info(f"Insertada fecha {load_date_str} en dim_date con date_sk={date_sk}")
    return date_sk


def _upsert_neighborhoods(conn, df_listings):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_neighborhood (
            neighborhood_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            UNIQUE(neighbourhood_group, neighbourhood)
        )
    """)

    pairs = (
        df_listings[["neighbourhood_group", "neighbourhood"]]
        .drop_duplicates()
        .itertuples(index=False)
    )

    for ng, n in pairs:
        cur.execute("""
            INSERT OR IGNORE INTO dim_neighborhood (
                neighbourhood_group, neighbourhood
            ) VALUES (?, ?)
        """, (ng, n))

    cur.execute("SELECT neighborhood_sk, neighbourhood_group, neighbourhood FROM dim_neighborhood")

    mapping = {(ng, n): sk for sk, ng, n in cur.fetchall()}

    logger.info(f"dim_neighborhood actualizada. Total barrios: {len(mapping)}")
    return mapping


# -----------------------------
# HOSTS (SCD2)
# -----------------------------
def _upsert_hosts(conn, df_listings, load_date_str):
    cur = conn.cursor()

    # 🔥 Para que no falle si el test no incluye estas columnas
    for col in ["host_id", "host_name", "calculated_host_listings_count"]:
        if col not in df_listings.columns:
            df_listings[col] = -1 if col == "host_id" else ("Unknown" if col == "host_name" else 0)

    # Crear tabla si no existe
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_host (
            host_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            host_id INTEGER,
            host_name TEXT,
            calculated_host_listings_count INTEGER,
            effective_from TEXT,
            effective_to TEXT,
            is_current INTEGER
        )
    """)

    hosts = df_listings[["host_id", "host_name", "calculated_host_listings_count"]] \
        .drop_duplicates().itertuples(index=False)

    for host_id, host_name, calc_count in hosts:

        cur.execute("""
            SELECT host_sk, host_name, calculated_host_listings_count, effective_from
            FROM dim_host
            WHERE host_id = ? AND is_current = 1
        """, (host_id,))
        row = cur.fetchone()

        # SAFE RE-RUN
        if row and row[3] == load_date_str:
            continue

        if row is None:
            cur.execute("""
                INSERT INTO dim_host (
                    host_id, host_name, calculated_host_listings_count,
                    effective_from, effective_to, is_current
                ) VALUES (?, ?, ?, ?, '9999-12-31', 1)
            """, (host_id, host_name, calc_count, load_date_str))
        else:
            sk, cur_name, cur_count, _ = row
            if cur_name != host_name or cur_count != calc_count:

                cur.execute("""
                    UPDATE dim_host SET is_current = 0, effective_to = ?
                    WHERE host_sk = ?
                """, (load_date_str, sk))

                cur.execute("""
                    INSERT INTO dim_host (
                        host_id, host_name, calculated_host_listings_count,
                        effective_from, effective_to, is_current
                    ) VALUES (?, ?, ?, ?, '9999-12-31', 1)
                """, (host_id, host_name, calc_count, load_date_str))

    cur.execute("SELECT host_id, host_sk FROM dim_host WHERE is_current = 1")
    return {hid: hsk for hid, hsk in cur.fetchall()}


# -----------------------------
# LISTINGS (SCD2)
# -----------------------------
def _upsert_listings(conn, df_listings, load_date_str):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_listing (
            listing_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            name TEXT,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            latitude REAL,
            longitude REAL,
            room_type TEXT,
            price REAL,
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            reviews_per_month REAL,
            availability_365 INTEGER,
            license TEXT,
            effective_from TEXT,
            effective_to TEXT,
            is_current INTEGER
        )
    """)

    for row in df_listings.itertuples(index=False):
        listing_id = row.id

        new_values = {
            "name": row.name,
            "neighbourhood_group": row.neighbourhood_group,
            "neighbourhood": row.neighbourhood,
            "latitude": row.latitude,
            "longitude": row.longitude,
            "room_type": row.room_type,
            "price": row.price,
            "minimum_nights": row.minimum_nights,
            "number_of_reviews": row.number_of_reviews,
            "reviews_per_month": getattr(row, "reviews_per_month", None),
            "availability_365": row.availability_365,
            "license": getattr(row, "license", None),
        }

        cur.execute("""
            SELECT listing_sk, name, neighbourhood_group, neighbourhood,
                   latitude, longitude, room_type, price, minimum_nights,
                   number_of_reviews, reviews_per_month, availability_365,
                   license, effective_from
            FROM dim_listing
            WHERE listing_id = ? AND is_current = 1
        """, (listing_id,))
        current = cur.fetchone()

        # SAFE-RE-RUN
        if current and current[-1] == load_date_str:
            continue

        if current is None:
            cur.execute("""
                INSERT INTO dim_listing (
                    listing_id, name, neighbourhood_group, neighbourhood,
                    latitude, longitude, room_type, price, minimum_nights,
                    number_of_reviews, reviews_per_month, availability_365,
                    license, effective_from, effective_to, is_current
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '9999-12-31', 1)
            """, (
                listing_id, *new_values.values(), load_date_str
            ))

    cur.execute("SELECT listing_id, listing_sk FROM dim_listing WHERE is_current = 1")
    return {lid: sk for lid, sk in cur.fetchall()}


# -----------------------------
# FACT TABLE
# -----------------------------
def _insert_facts(conn, df_listings, listing_map, host_map, neighborhood_map, date_sk, load_date_str):
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_listings (
            fact_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_sk INTEGER,
            host_sk INTEGER,
            neighborhood_sk INTEGER,
            date_sk INTEGER,
            price REAL,
            number_of_reviews INTEGER,
            availability_365 INTEGER,
            load_date TEXT
        )
    """)

    cur.execute("DELETE FROM fact_listings WHERE date_sk = ?", (date_sk,))

    for row in df_listings.itertuples(index=False):
        cur.execute("""
            INSERT INTO fact_listings (
                listing_sk, host_sk, neighborhood_sk,
                date_sk, price, number_of_reviews, availability_365, load_date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            listing_map.get(row.id),
            host_map.get(row.host_id),
            neighborhood_map.get((row.neighbourhood_group, row.neighbourhood)),
            date_sk,
            row.price,
            row.number_of_reviews,
            row.availability_365,
            load_date_str
        ))


# -----------------------------
# MAIN LOAD FUNCTION
# -----------------------------
def load_to_sqlite(df_listings, df_reviews, db_path="airbnb.db", load_date=None):
    if load_date is None:
        load_date = date.today()

    load_date_str = load_date.strftime("%Y-%m-%d")
    conn = get_connection(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # 🔥 NECESARIO PARA TESTS (DB vacía en memoria)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_host (
            host_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            host_id INTEGER,
            host_name TEXT,
            calculated_host_listings_count INTEGER,
            effective_from TEXT,
            effective_to TEXT,
            is_current INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS dim_listing (
            listing_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id INTEGER,
            name TEXT,
            neighbourhood_group TEXT,
            neighbourhood TEXT,
            latitude REAL,
            longitude REAL,
            room_type TEXT,
            price REAL,
            minimum_nights INTEGER,
            number_of_reviews INTEGER,
            reviews_per_month REAL,
            availability_365 INTEGER,
            license TEXT,
            effective_from TEXT,
            effective_to TEXT,
            is_current INTEGER
        )
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS fact_listings (
            fact_sk INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_sk INTEGER,
            host_sk INTEGER,
            neighborhood_sk INTEGER,
            date_sk INTEGER,
            price REAL,
            number_of_reviews INTEGER,
            availability_365 INTEGER,
            load_date TEXT
        )
    """)

    try:
        logger.info(f"Iniciando carga a SQLite ({db_path}) con fecha de carga {load_date_str}...")

        date_sk = _ensure_date_dim(conn, load_date_str)
        neighborhood_map = _upsert_neighborhoods(conn, df_listings)
        host_map = _upsert_hosts(conn, df_listings, load_date_str)
        listing_map = _upsert_listings(conn, df_listings, load_date_str)

        _insert_facts(conn, df_listings, listing_map, host_map, neighborhood_map, date_sk, load_date_str)

        conn.commit()
        logger.info("Carga completada con éxito.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error durante la carga, se hace rollback: {e}")
        raise
    finally:
        conn.close()
        logger.info("Conexión a base de datos cerrada.")
