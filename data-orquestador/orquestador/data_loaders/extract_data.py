if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

@data_loader
def load_data(*args, **kwargs):
    year = kwargs.get('year', 2025)
    month = kwargs.get('month', '01')

    URL = f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet'

    datos_crudos = pd.read_parquet(URL)

    return datos_crudos


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'