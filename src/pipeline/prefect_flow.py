from prefect import flow, task
from datetime import datetime

# Importamos tus funciones del pipeline
from src.pipeline.extract import load_listings, load_reviews
from src.pipeline.validate import validate_data
from src.pipeline.transform import transform_data
from src.pipeline.load import load_to_sqlite


@task
def extract_task(data_dir):
    listings = load_listings(f"{data_dir}/listings.csv")
    reviews = load_reviews(f"{data_dir}/reviews.csv")
    return listings, reviews


@task
def validate_task(df_listings, df_reviews):
    return validate_data(df_listings, df_reviews)


@task
def transform_task(df_listings, df_reviews):
    return transform_data(df_listings, df_reviews)


@task
def load_task(df_listings_t, df_reviews_t, db_path):
    load_to_sqlite(df_listings_t, df_reviews_t, db_path)


@flow(name="airbnb_etl_flow")
def airbnb_etl_flow(data_dir="data", db_path="airbnb.db"):
    print("Ejecutando ETL con Prefect...")

    df_listings, df_reviews = extract_task(data_dir)
    report = validate_task(df_listings, df_reviews)
    print("Resultado validación:", report["status"])

    df_listings_t, df_reviews_t = transform_task(df_listings, df_reviews)
    load_task(df_listings_t, df_reviews_t, db_path)

    print("ETL completado desde Prefect!")


if __name__ == "__main__":
    airbnb_etl_flow()
