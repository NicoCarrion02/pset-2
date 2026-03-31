from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

import pandas as pd
from typing import List, Dict, Any

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(sources: List[Dict[str, Any]], **kwargs) -> None:
    schema_name = 'raw'  # Specify the name of the schema to export data to
    table_name = 'ny_taxi_trips'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    chunksize = int(kwargs.get("chunksize", 500000))
    first_write = True

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for source in sources:
            year = int(source["year"])
            month = int(source["month"])
            url = source["url"]

            try:
                monthly_df = pd.read_parquet(url)
            except Exception as exc:
                print(f"\n[WARN] Failed to read {year}-{month:02d}: {exc}")
                continue
            
            monthly_df.columns = [col.lower() for col in monthly_df.columns]
    
            monthly_df.rename(columns={
                'vendorid': 'vendor_id',
                'ratecodeid': 'rate_code_id',
                'pulocationid': 'pu_location_id',
                'dolocationid': 'do_location_id'
            }, inplace = True)
            
            rows = monthly_df.shape[0]

            for i in range(0, rows, chunksize):
                loader.export(
                    monthly_df.iloc[i:i + chunksize],
                    schema_name,
                    table_name,
                    index=False,  # Specifies whether to include index in exported table
                    if_exists='replace' if first_write else 'append',  # Specify resolution policy if table name already exists
                )

                first_write = False
            
            print(f"\n[OK] Loaded {year}-{month:02d} ({rows} rows)")
