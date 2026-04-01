from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(data: DataFrame, **kwargs) -> None:
    schema_name = 'raw'  # Specify the name of the schema to export data to
    table_name = 'ny_taxi_trips'  # Specify the name of the table to export data to
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    execution_date = kwargs.get('execution_date')
    if execution_date is None:
        raise ValueError("Failed to get execution_date")

    current_year = int(execution_date.year)
    current_month = int(execution_date.month)

    chunksize = int(kwargs.get("chunksize", 500000))
    rows = data.shape[0]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        print(f"[INFO] Exporting data for {current_year}-{current_month:02d} to PostgreSQL...")

        delete_query = f"""
            DELETE FROM {schema_name}.{table_name}
            WHERE trip_year = {current_year}
              AND trip_month = {current_month};
        """
        try:
            loader.execute(delete_query)
            table_ready = True
            print(f"[OK] Deleted existing records for {current_year}-{current_month:02d} (if any)")
        except Exception as exc:
            loader.conn.rollback()
            print(
                f"[WARN] Delete step skipped for {current_year}-{current_month:02d}: {exc}. "
                "Will rebuild/append table data instead."
            )
            table_ready = False

        for i in range(0, rows, chunksize):
            chunk = data.iloc[i:i + chunksize]

            loader.export(
                chunk,
                schema_name,
                table_name,
                index=False,
                if_exists='append' if table_ready or i > 0 else 'replace',
            )
            
            print(f"[OK] Exported chunk {i // chunksize + 1} ({chunk.shape[0]} rows) for {current_year}-{current_month:02d}")

        print(f"[OK] Loaded {current_year}-{current_month:02d} ({rows} rows)")
