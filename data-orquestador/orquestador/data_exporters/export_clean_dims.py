from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.postgres import Postgres
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_dimensions_to_postgres(output: dict, **kwargs) -> None:
    dims = output['dimensions']  # Solo tomamos las dimensiones
    schema_name = 'clean'
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Postgres.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for dim_name, df_dim in dims.items():
            print(f"[INFO] Exporting dimension {dim_name} ({df_dim.shape[0]} rows) to PostgreSQL...")
            try:
                # Reemplaza toda la tabla
                loader.export(
                    df_dim,
                    schema_name,
                    dim_name,
                    index=False,
                    if_exists='replace',
                )
                print(f"[OK] Dimension {dim_name} exported successfully")
            except Exception as exc:
                loader.conn.rollback()
                print(f"[ERROR] Failed to export dimension {dim_name}: {exc}")
                