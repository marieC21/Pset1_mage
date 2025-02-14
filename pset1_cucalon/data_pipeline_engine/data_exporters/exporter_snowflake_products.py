from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_products_to_snowflake(df: DataFrame, **kwargs) -> None:
    """
    Exporta los datos de la tabla PRODUCTS a Snowflake en la base de datos RAPIDB y el esquema RAW.
    """

    table_name = 'PRODUCTS'  # Nombre de la tabla en Snowflake
    database = 'RAPIDB'  # Base de datos en Snowflake
    schema = 'RAW'  # Esquema en Snowflake
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with Snowflake.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        loader.export(
            df,
            table_name,
            database,
            schema,
            if_exists='replace',  # Reemplaza la tabla si ya existe
        )