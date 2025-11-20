import pandas as pd
from src.utils.logger import get_logger

logger = get_logger(__name__)


def transform_data(df_listings, df_reviews):
    """
    Transformaciones aplicadas a listings, incorporando datos de reviews.
    Devuelve SOLO df_listings transformado.
    """
    logger.info("Iniciando transformaciones...")

    df = df_listings.copy()

    # === Limpieza de tipos ===
    logger.info("Convirtiendo tipos y limpiando nulos...")

    num_cols = ["price", "reviews_per_month", "number_of_reviews"]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df.fillna({"name": "Unknown", "host_name": "Unknown"}, inplace=True)

    # === review_count ===
    logger.info("Agregando review_count a listings...")

    df_rev = df_reviews.groupby("listing_id").size().reset_index(name="review_count")
    df = df.merge(df_rev, how="left", left_on="id", right_on="listing_id")
    df["review_count"] = df["review_count"].fillna(0).astype(int)

    # Columnas mínimas necesarias en casos faltantes
    if "host_id" not in df.columns:
        df["host_id"] = -1
    if "host_name" not in df.columns:
        df["host_name"] = "Unknown"
    if "calculated_host_listings_count" not in df.columns:
        df["calculated_host_listings_count"] = 0

    logger.info("Transformaciones completadas.")
    return df
