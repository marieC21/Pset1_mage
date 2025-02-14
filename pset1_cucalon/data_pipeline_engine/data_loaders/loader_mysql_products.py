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
def load_products_from_mysql(*args, **kwargs):
    """
    Carga los datos de la tabla 'products' desde MySQL de manera optimizada.
    """

    query = 'SELECT product_id, product_name, aisle_id, department_id FROM products;'  # Limita carga inicial
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        chunks = []
        for chunk in loader.load(query, chunksize=20000):  # Carga en lotes de 20k
            chunks.append(chunk)
    
    return pd.concat(chunks, ignore_index=True)  # Une los lotes en un solo DataFrame

@test
def test_output(output, *args) -> None:
    """
    Prueba para verificar que la salida no es None y contiene datos.
    """
    assert output is not None, 'El output está vacío'
    assert not output.empty, 'El DataFrame está vacío'
    assert 'product_id' in output.columns, "Falta la columna 'product_id'"
    assert 'product_name' in output.columns, "Falta la columna 'product_name'"
    assert 'aisle_id' in output.columns, "Falta la columna 'aisle_id'"
    assert 'department_id' in output.columns, "Falta la columna 'department_id'"