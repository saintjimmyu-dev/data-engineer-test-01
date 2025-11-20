import pandas as pd
import sqlite3
from src.pipeline.load import load_to_sqlite

def test_load_sqlite():
    df_listings = pd.DataFrame({
        "id": [1],
        "name": ["Test"],
        "neighbourhood_group": ["A"],
        "neighbourhood": ["B"],
        "latitude": [1.0],
        "longitude": [2.0],
        "room_type": ["Private room"],
        "price": [100],
        "minimum_nights": [2],
        "number_of_reviews": [5],
        "reviews_per_month": [1.0],
        "availability_365": [100],
        "license": ["123"],
        "review_count": [5],
    })

    df_reviews = pd.DataFrame({
        "listing_id": [1]
    })

    # SQLite en memoria para no tocar la BD real
    load_to_sqlite(df_listings, df_reviews, db_path=":memory:")

    # Si no lanza error → test OK
    assert True
