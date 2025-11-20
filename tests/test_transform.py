import pandas as pd
from src.pipeline.transform import transform_data

def test_transform_data():
    df_listings = pd.DataFrame({
        "id": [1],
        "price": ["100"],
        "number_of_reviews": [10],
        "reviews_per_month": [2.5],
        "availability_365": [200],
        "neighbourhood_group": ["A"],
        "neighbourhood": ["X"],
        "latitude": [10.0],
        "longitude": [20.0],
        "room_type": ["Private"]
    })

    df_reviews = pd.DataFrame({
        "listing_id": [1]
    })

    df_t = transform_data(df_listings, df_reviews)

    assert isinstance(df_t, pd.DataFrame)
    assert "review_count" in df_t.columns
    assert df_t.loc[0, "review_count"] == 1
