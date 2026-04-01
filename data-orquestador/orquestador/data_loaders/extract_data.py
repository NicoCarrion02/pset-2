if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    execution_date = kwargs.get('execution_date')
    if execution_date is None:
        raise ValueError("Failed to get execution_date")

    year = execution_date.year
    month = execution_date.month

    url = (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/"
        f"yellow_tripdata_{year}-{month:02d}.parquet"
    )

    df = pd.read_parquet(url)

    # Metadatos para idempotencia y trazabilidad
    df["trip_year"] = year
    df["trip_month"] = month

    return df

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'