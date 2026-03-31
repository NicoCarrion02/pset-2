if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from typing import List, Dict, Any

@data_loader
def load_data(*args, **kwargs) -> List[Dict[str, Any]]:
    start_year = int(kwargs.get('start_year', 2025)) # Solo se carga un año por gran tamaño de datos
    end_year = int(kwargs.get('end_year', 2025))
    months = kwargs.get('months', list(range(1, 13)))

    sources = []
    for year in range(start_year, end_year + 1):
        for month in months:
            month = int(month)
            url = (
                "https://d37ci6vzurychx.cloudfront.net/trip-data/"
                f'yellow_tripdata_{year}-{month:02d}.parquet'
            )
            sources.append({
                "year": year,
                "month": month,
                "url": url
            })

    return sources

@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'