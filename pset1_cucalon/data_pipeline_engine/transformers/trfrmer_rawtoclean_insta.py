if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import snowflake.connector
import numpy as np

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Transformer para limpiar la tabla 'INSTACART_ORDERS' y guardarla en CLEAN.INSTACART_ORDERS_CLEAN.
    """
    print("🔄 Conectando a Snowflake para extraer datos de INSTACART_ORDERS...")

    # Conectar a Snowflake
    try:
        conn = snowflake.connector.connect(
            user="SERVICE_USER",
            password="BF2001$$ash",
            account="ADAPFIL-NRB91509",
            warehouse="COMPUTE_WH",
            database="RAPIDB",
            schema="RAW"
        )
        print("✅ Conexión exitosa a Snowflake")
        
        # 📌 Extraer los datos de RAW.INSTACART_ORDERS con nombres de columna en minúsculas
        query = 'SELECT "order_id", "user_id", "order_number", "order_dow", "order_hour_of_day", "days_since_prior_order" FROM RAW.INSTACART_ORDERS;'
        orders_df = pd.read_sql(query, conn)

        print("📌 Datos extraídos de RAW.INSTACART_ORDERS")

    except Exception as e:
        raise Exception(f"❌ Error al conectar a Snowflake: {e}")

    # Convertir nombres de columnas a minúsculas
    orders_df.columns = orders_df.columns.str.lower()

    # Validar columnas
    required_columns = {'order_id', 'user_id', 'order_number', 'order_dow', 'order_hour_of_day', 'days_since_prior_order'}
    missing_columns = required_columns - set(orders_df.columns)
    if missing_columns:
        raise KeyError(f"❌ Faltan las siguientes columnas en INSTACART_ORDERS: {missing_columns}")

    # Convertir tipos de datos
    for col in ['order_id', 'user_id', 'order_number', 'order_dow', 'order_hour_of_day']:
        orders_df[col] = pd.to_numeric(orders_df[col], errors='coerce').astype('Int64')

    orders_df['days_since_prior_order'] = pd.to_numeric(orders_df['days_since_prior_order'], errors='coerce').fillna(0).astype('float64')

    # Eliminar duplicados
    orders_df = orders_df.drop_duplicates(subset=['order_id'])

    # 🔄 Guardar la versión limpia en CLEAN.INSTACART_ORDERS_CLEAN por lotes
    try:
        conn.cursor().execute("USE SCHEMA CLEAN;")  # Cambiar a CLEAN schema
        print("📌 Usando schema CLEAN")

        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS CLEAN.INSTACART_ORDERS_CLEAN (
            order_id INT,
            user_id INT,
            order_number INT,
            order_dow INT,
            order_hour_of_day INT,
            days_since_prior_order FLOAT
        );
        """
        conn.cursor().execute(create_table_query)
        print("✅ Tabla INSTACART_ORDERS_CLEAN creada/verificada en CLEAN")

        # Dividir el DataFrame en lotes de 50,000 filas para evitar el error
        batch_size = 50000
        num_batches = int(np.ceil(len(orders_df) / batch_size))

        print(f"🔄 Insertando datos en lotes de {batch_size} filas ({num_batches} lotes en total)...")

        insert_query = """
        INSERT INTO CLEAN.INSTACART_ORDERS_CLEAN (order_id, user_id, order_number, order_dow, order_hour_of_day, days_since_prior_order) 
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        cursor = conn.cursor()
        for i in range(num_batches):
            batch = orders_df.iloc[i * batch_size : (i + 1) * batch_size].values.tolist()
            cursor.executemany(insert_query, batch)
            print(f"✅ Lote {i+1}/{num_batches} insertado con {len(batch)} filas")

        print("✅ Todos los lotes insertados correctamente en CLEAN.INSTACART_ORDERS_CLEAN")

    except Exception as e:
        raise Exception(f"❌ Error al guardar en CLEAN.INSTACART_ORDERS_CLEAN: {e}")

    finally:
        # Cerrar conexión
        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    return orders_df


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Test para validar la limpieza de datos.
    """
    assert output is not None, '❌ El output está vacío'
    assert isinstance(output, pd.DataFrame), '❌ El output no es un DataFrame'
    assert 'order_id' in output.columns, '❌ order_id no está en las columnas'
    assert output['order_id'].dtype in ['int64', 'Int64'], '❌ order_id no tiene el tipo correcto'
    assert output['user_id'].dtype in ['int64', 'Int64'], '❌ user_id no tiene el tipo correcto'
    assert output['order_number'].dtype in ['int64', 'Int64'], '❌ order_number no tiene el tipo correcto'
    assert output['order_dow'].dtype in ['int64', 'Int64'], '❌ order_dow no tiene el tipo correcto'
    assert output['order_hour_of_day'].dtype in ['int64', 'Int64'], '❌ order_hour_of_day no tiene el tipo correcto'
    assert output['days_since_prior_order'].dtype == 'float64', '❌ days_since_prior_order no tiene el tipo correcto'
    assert output['days_since_prior_order'].isnull().sum() == 0, '❌ Hay valores nulos en days_since_prior_order'
    assert output.duplicated(subset=['order_id']).sum() == 0, '❌ Aún hay duplicados en order_id'

