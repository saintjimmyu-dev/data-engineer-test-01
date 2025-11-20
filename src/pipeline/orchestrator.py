import pandas as pd
from src.pipeline.extract import load_listings, load_reviews
from src.pipeline.validate import validate_data
from src.pipeline.transform import transform_data
from src.pipeline.load import load_to_sqlite
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_pipeline():

    try:
        logger.info("=====================================================")
        logger.info(" INICIANDO PIPELINE ETL AIRBNB")
        logger.info("=====================================================")

        # --------------------
        # 1. EXTRACT
        # --------------------
        logger.info("1) Extrayendo datos...")

        df_listings = load_listings()
        df_reviews = load_reviews()

        logger.info("Extracción completada.")

        # --------------------
        # 2. VALIDATE
        # --------------------
        logger.info("2) Validando datos...")

        status, report = validate_data(df_listings, df_reviews)
        logger.info(f"Resultado validación: Status = {status}")

        # --------------------
        # 3. TRANSFORM
        # --------------------
        logger.info("3) Transformando datos...")

        df_listings_t = transform_data(df_listings, df_reviews)

        logger.info("Transformación completada.")

        # --------------------
        # 4. LOAD
        # --------------------
        logger.info("4) Cargando a SQLite...")

        load_to_sqlite(df_listings_t, df_reviews)

        logger.info("Carga completada exitosamente.")
        logger.info("PIPELINE FINALIZADO ✔")

    except Exception as e:
        logger.exception(" ERROR GRAVE DURANTE EL PIPELINE:")
        raise e
