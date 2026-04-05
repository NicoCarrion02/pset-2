from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

import pandas as pd

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_facts_to_postgres(output: dict, **kwargs) -> None:
    data = output['facts']  # Solo tomamos los facts
    schema_name = 'clean'
    table_name = 'fact_taxi_trip'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    execution_date = kwargs.get('execution_date')
    if execution_date is None:
        raise ValueError("Failed to get execution_date")

    year = execution_date.year
    month = execution_date.month
    hour = execution_date.hour
    
    # day_partitions = 4 # usado para 2 backfills en la mañana
    day_partitions = 6 # usado para 3 backfills en la tarde
    
    hours_per_partition = 24 / day_partitions
    time_partition_index  = hour // hours_per_partition

    chunksize = int(kwargs.get("chunksize", 500000))
    rows = data.shape[0]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        print(
            f"[INFO] Exporting facts for {year}-{month:02d} "
            f"(partition {time_partition_index + 1}/{day_partitions}) to PostgreSQL..."
        )

        # Elimina solo los registros del mismo mes/año
        delete_query = f"""
            DELETE FROM {schema_name}.{table_name}
            WHERE trip_year = {year}
              AND trip_month = {month}
              AND FLOOR(EXTRACT(HOUR FROM tpep_pickup_datetime) / {hours_per_partition}) = {time_partition_index};
        """
        try:
            loader.execute(delete_query)
            table_ready = True
            print(
                f"[OK] Deleted (if any) existing records for {year}-{month:02d} "
                f"(partition {time_partition_index + 1}/{day_partitions})"
            )
        except Exception as exc:
            loader.conn.rollback()
            print(
                f"[WARN] Delete step skipped for {year}-{month:02d} "
                f"(partition {time_partition_index + 1}/{day_partitions}): {exc}. "
                "Will rebuild/append table data instead."
            )
            table_ready = False

        for i in range(0, rows, chunksize):
            chunk = data.iloc[i:i + chunksize].copy()

            for col in ['tpep_pickup_datetime', 'tpep_dropoff_datetime']:
                chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

            loader.export(
                chunk,
                schema_name,
                table_name,
                index=False,
                if_exists='append' if table_ready or i > 0 else 'replace',
            )
            
            print(f"[OK] Exported chunk {i // chunksize + 1} ({chunk.shape[0]} rows)")

        print(f"[OK] Loaded {rows} rows into {schema_name}.{table_name}")
        