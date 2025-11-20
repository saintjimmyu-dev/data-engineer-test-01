import pandas as pd
import json
import os
from src.utils.logger import get_logger

logger = get_logger(__name__)


def validate_data(df_listings, df_reviews, output_path="../output/data_quality_report.json"):
    logger.info("Iniciando validaciones de calidad de datos...")

    report = {
        "listings": {},
        "reviews": {},
        "integrity": {},
        "status": "OK",
        "null_critical": {
            "listings": {}
        }
    }

    # ------------------------------------
    # 1. VALIDACIÓN DE ESQUEMA
    # ------------------------------------
    expected_listings_cols = [
        "id", "name", "host_id", "host_name", "neighbourhood_group",
        "neighbourhood", "latitude", "longitude", "room_type", "price",
        "minimum_nights", "number_of_reviews", "last_review",
        "reviews_per_month", "calculated_host_listings_count",
        "availability_365", "number_of_reviews_ltm", "license"
    ]

    missing_cols = [c for c in expected_listings_cols if c not in df_listings.columns]
    report["listings"]["missing_columns"] = missing_cols

    if missing_cols:
        logger.warning(f"Columnas faltantes en listings: {missing_cols}")
        report["status"] = "WARNING"

    # ------------------------------------
    # 2. NULLS CRÍTICOS (formato requerido por tests)
    # ------------------------------------
    critical_cols = ["id", "host_id", "latitude", "longitude", "price"]

    null_dict = {}
    for col in critical_cols:
        if col in df_listings.columns:
            null_dict[col] = int(df_listings[col].isna().sum())
        else:
            null_dict[col] = None

    report["null_critical"]["listings"] = null_dict

    if any(v and v > 0 for v in null_dict.values()):
        logger.warning("Hay nulos críticos en listings.")
        report["status"] = "WARNING"

    # ------------------------------------
    # 3. DUPLICADOS
    # ------------------------------------
    dup_count = df_listings["id"].duplicated().sum()
    report["listings"]["duplicate_ids"] = int(dup_count)

    if dup_count > 0:
        report["status"] = "WARNING"

    # ------------------------------------
    # 4. PRECIOS INVÁLIDOS
    # ------------------------------------
    if "price" in df_listings.columns:
        invalid_prices = int((df_listings["price"] < 0).sum())
    else:
        invalid_prices = None

    report["listings"]["invalid_prices"] = invalid_prices

    if invalid_prices and invalid_prices > 0:
        report["status"] = "WARNING"

    # ------------------------------------
    # 5. REVIEWS HUÉRFANAS
    # ------------------------------------
    listing_ids = set(df_listings["id"].dropna())
    orphan_reviews = df_reviews[~df_reviews["listing_id"].isin(listing_ids)]
    report["integrity"]["orphan_reviews"] = len(orphan_reviews)

    if len(orphan_reviews) > 0:
        report["status"] = "WARNING"

    # ------------------------------------
    # GUARDAR JSON
    # ------------------------------------
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    logger.info("Validaciones completadas. Reporte generado en:")
    logger.info(output_path)

    return report["status"], report
