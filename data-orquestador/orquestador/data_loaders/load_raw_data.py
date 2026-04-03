from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import pandas as pd

def safe_load_table(loader, table_name, columns):
    try:
        df = loader.load(f"SELECT * FROM {table_name}")
        return df
    except Exception as e:
        loader.conn.rollback()
        return pd.DataFrame(columns=columns)

@data_loader
def load_data_from_postgres(*args, **kwargs):
    execution_date = kwargs.get('execution_date')
    if execution_date is None:
        raise ValueError("Failed to get execution_date")

    year = execution_date.year
    month = execution_date.month

    query_raw = f"""
        SELECT
            *
        FROM raw.ny_taxi_trips
        WHERE trip_year = {year}
          AND trip_month = {month}
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        df_raw = loader.load(query_raw)

        df_vendor = safe_load_table(loader, "clean.dim_vendor", ["vendor_id", "vendor_name"])
        df_payment_type = safe_load_table(loader, "clean.dim_payment_type", ["payment_type_id", "payment_type_name"])
        df_rate_code = safe_load_table(loader, "clean.dim_rate_code", ["rate_code_id", "rate_code_name"])
        df_pu_location = safe_load_table(loader, "clean.dim_pickup_location", ["pu_location_id"])
        df_do_location = safe_load_table(loader, "clean.dim_dropoff_location", ["do_location_id"])
    
    return {
        'raw': df_raw,
        'dimensions': {
            'dim_vendor': df_vendor,
            'dim_payment_type': df_payment_type,
            'dim_rate_code': df_rate_code,
            'dim_pickup_location': df_pu_location,
            'dim_dropoff_location': df_do_location,
        }
    }


@test
def test_output(output, *args) -> None:
    assert output is not None, 'The output is undefined'
