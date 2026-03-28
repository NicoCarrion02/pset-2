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

    chunksize = 500000
    rows = data.shape[0]

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            data.iloc[0:chunksize],
            schema_name,
            table_name,
            index=False,  # Specifies whether to include index in exported table
            if_exists='replace',  # Specify resolution policy if table name already exists
        )

        for i in range(chunksize, rows, chunksize):
            loader.export(
                data.iloc[i:i+chunksize],
                schema_name,
                table_name,
                index=False,  # Specifies whether to include index in exported table
                if_exists='append',  # Specify resolution policy if table name already exists
            )
