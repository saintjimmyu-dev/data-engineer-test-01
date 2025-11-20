import sqlite3
from src.utils.logger import get_logger

logger = get_logger(__name__)

def get_connection(db_path="airbnb.db"):
    """
    Crea y devuelve una conexión a SQLite con foreign keys activadas.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    logger.info(f"Conexión abierta a la base de datos: {db_path}")
    return conn
