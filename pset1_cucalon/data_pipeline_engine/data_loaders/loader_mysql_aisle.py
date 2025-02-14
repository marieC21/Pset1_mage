from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
import pandas as pd

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_aisles_from_mysql(*args, **kwargs):
    """
    Carga los datos de la tabla 'aisles' desde MySQL y los devuelve como un DataFrame.
    """

    query = 'SELECT * FROM aisles;'  # Consulta para obtener todos los datos de la tabla aisles
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        data = loader.load(query)
    
    return pd.DataFrame(data)  # Retorna los datos en formato DataFrame

@test
def test_output(output, *args) -> None:
    """
    Prueba para verificar que la salida no es None y contiene datos.
    """
    assert output is not None, 'El output está vacío'
    assert not output.empty, 'El DataFrame está vacío'
    assert 'aisle_id' in output.columns, "Falta la columna 'aisle_id'"
    assert 'aisle' in output.columns, "Falta la columna 'aisle'"
