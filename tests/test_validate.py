import pandas as pd
from src.pipeline.validate import validate_data

def test_validate_warning():
    df_listings = pd.DataFrame({"id": [1, None]})
    df_reviews = pd.DataFrame({"listing_id": [1]})

    status, report = validate_data(df_listings, df_reviews)

    assert status == "WARNING"
    assert "listings" in report["null_critical"]
