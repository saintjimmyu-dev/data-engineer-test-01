import os
import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Ruta correcta (según tu captura)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

DEFAULT_LISTINGS_PATH = os.path.join(DATA_DIR, "listings.csv")
DEFAULT_REVIEWS_PATH = os.path.join(DATA_DIR, "reviews.csv")


def load_listings(path=None):
    """
    Carga listings.csv desde la carpeta data del proyecto.
    """
    if path is None:
        path = DEFAULT_LISTINGS_PATH

    logger.info(f"Cargando listings desde {path}")
    df = pd.read_csv(path)
    return df


def load_reviews(path=None):
    """
    Carga reviews.csv desde la carpeta data del proyecto.
    """
    if path is None:
        path = DEFAULT_REVIEWS_PATH

    logger.info(f"Cargando reviews desde {path}")
    df = pd.read_csv(path)
    return df
