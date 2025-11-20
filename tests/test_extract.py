import pandas as pd
from src.pipeline.extract import load_listings, load_reviews

def test_load_listings():
    df = load_listings("data/listings.csv")
    assert isinstance(df, pd.DataFrame)
    assert "id" in df.columns
    assert len(df) > 0

def test_load_reviews():
    df = load_reviews("data/reviews.csv")
    assert isinstance(df, pd.DataFrame)
    assert "listing_id" in df.columns
    assert len(df) > 0
