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
def load_data_from_mysql(*args, **kwargs):
    """
    Carga todas las tablas de la base de datos 'instacart_db' en MySQL.
    """

    # Obtiene la ruta de configuración
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'  

    tables = ['aisles', 'departments', 'products', 'instacart_orders', 'order_products']
    data = {}

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        for table in tables:
            query = f"SELECT * FROM {table};"
            try:
                df = loader.load(query)

                # Verificar si realmente es un DataFrame
                if not isinstance(df, pd.DataFrame):
                    raise ValueError(f"Error: {table} no devolvió un DataFrame.")

                # Convertir a JSON para evitar referencias circulares
                data[table] = df.astype(str)  # Convertir todos los valores a string temporalmente

                print(f"✔ Cargada tabla {table} con {df.shape[0]} filas y {df.shape[1]} columnas.")
            except Exception as e:
                print(f"❌ Error al cargar la tabla {table}: {e}")
                data[table] = None  # Marcar la tabla con error

    return data  # Devuelve el diccionario con DataFrames convertidos a strings
