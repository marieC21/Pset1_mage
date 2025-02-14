from mage_ai.data_preparation.repo_manager import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.mysql import MySQL
from os import path
import pandas as pd
import psutil  # Monitoreo de memoria RAM
import time  # Medir tiempos de ejecución

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

@data_loader
def load_order_products_from_mysql(*args, **kwargs):
    """
    Carga la tabla 'order_products' desde MySQL en lotes y devuelve un solo DataFrame.
    Monitorea memoria RAM y tiempo de ejecución en la terminal.
    """

    query = '''
        SELECT order_id, product_id, add_to_cart_order, reordered
        FROM order_products;
    '''  

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    batch_size = 500000  # Límite de filas por lote
    chunks_list = []  # Lista para almacenar los DataFrames parciales
    total_rows = 0  # Contador de filas totales
    start_time = time.time()  # Iniciar cronómetro

    print("\n📢 Iniciando carga de datos desde MySQL...\n")

    with MySQL.with_config(ConfigFileLoader(config_path, config_profile)) as loader:
        chunks = loader.load(query, chunksize=batch_size)
        
        for i, chunk in enumerate(chunks):
            chunk_size = len(chunk)
            total_rows += chunk_size
            chunks_list.append(chunk)

            # Monitoreo de memoria RAM
            ram_usage = psutil.virtual_memory().percent

            print(f"🔹 Lote {i+1}: {chunk_size} filas cargadas | Total acumulado: {total_rows} filas")
            print(f"📊 Uso de RAM: {ram_usage}%\n")

    # Unir todos los lotes en un único DataFrame
    df = pd.concat(chunks_list, ignore_index=True)  

    end_time = time.time()  # Finalizar cronómetro
    elapsed_time = end_time - start_time

    print("\n✅ Carga completada.")
    print(f"📦 Total de registros cargados: {total_rows}")
    print(f"⏳ Tiempo total: {elapsed_time:.2f} segundos")
    print(f"📊 Uso final de RAM: {psutil.virtual_memory().percent}%\n")

    return df  # Mage AI ahora recibirá un único DataFrame, no un iterador

@test
def test_output(output, *args) -> None:
    """
    Prueba rápida: verifica que los datos estén presentes y tengan estructura correcta.
    """
    assert output is not None, 'Error: El DataFrame está vacío'
    assert not output.empty, 'Error: No se cargaron datos'
    assert set(['order_id', 'product_id', 'add_to_cart_order', 'reordered']).issubset(output.columns), \
        'Error: Faltan columnas clave'

