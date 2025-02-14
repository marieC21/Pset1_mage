if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import snowflake.connector

CHUNK_SIZE = 50000  # 🔥 Ajusta el tamaño del chunk para optimizar velocidad

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Transformer para limpiar la tabla 'ORDER_PRODUCTS' y cargarla en CLEAN.ORDER_PRODUCTS_CLEAN usando Snowflake Connector con chunks.
    """
    print("🔄 Conectando a Snowflake para extraer datos de ORDER_PRODUCTS...")

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

        query = "SELECT * FROM RAW.ORDER_PRODUCTS;"
        order_products_df = pd.read_sql(query, conn)

        # Cerrar conexión
        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    except Exception as e:
        raise Exception(f"❌ Error al conectar a Snowflake: {e}")

    # Convertir nombres de columnas a mayúsculas para evitar errores
    order_products_df.columns = order_products_df.columns.str.upper()

    # Validar que las columnas necesarias existen
    required_columns = {'ORDER_ID', 'PRODUCT_ID', 'ADD_TO_CART_ORDER', 'REORDERED'}
    missing_columns = required_columns - set(order_products_df.columns)
    if missing_columns:
        raise KeyError(f"❌ Faltan las siguientes columnas en ORDER_PRODUCTS: {missing_columns}")

    # Reemplazar valores nulos en ADD_TO_CART_ORDER con -1
    order_products_df['ADD_TO_CART_ORDER'] = order_products_df['ADD_TO_CART_ORDER'].fillna(-1)

    # Cambiar tipos de datos
    order_products_df = order_products_df.astype({
        'ORDER_ID': 'int64',
        'PRODUCT_ID': 'int64',
        'ADD_TO_CART_ORDER': 'float64',
        'REORDERED': 'int64'
    })

    print("✅ Limpieza completada. Datos listos para insertar en CLEAN.ORDER_PRODUCTS_CLEAN.")

    # 🔄 Guardar en Snowflake en chunks
    try:
        conn = snowflake.connector.connect(
            user="SERVICE_USER",
            password="BF2001$$ash",
            account="ADAPFIL-NRB91509",
            warehouse="COMPUTE_WH",
            database="RAPIDB",
            schema="CLEAN"
        )
        cursor = conn.cursor()

        # Crear la tabla si no existe
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS CLEAN.ORDER_PRODUCTS_CLEAN (
                ORDER_ID INT,
                PRODUCT_ID INT,
                ADD_TO_CART_ORDER FLOAT,
                REORDERED INT
            );
        """)
        print("✅ Tabla ORDER_PRODUCTS_CLEAN creada/verificada en CLEAN.")

        # Insertar datos en chunks
        total_rows = len(order_products_df)
        for i in range(0, total_rows, CHUNK_SIZE):
            chunk = order_products_df.iloc[i:i+CHUNK_SIZE]

            # Convertir a lista de tuplas para insert masivo
            values = [tuple(x) for x in chunk.to_numpy()]

            insert_query = """
                INSERT INTO CLEAN.ORDER_PRODUCTS_CLEAN (ORDER_ID, PRODUCT_ID, ADD_TO_CART_ORDER, REORDERED)
                VALUES (%s, %s, %s, %s)
            """
            cursor.executemany(insert_query, values)
            print(f"✅ Insertadas {len(chunk)} filas en Snowflake... ({i+len(chunk)}/{total_rows})")

        conn.commit()
        cursor.close()
        conn.close()
        print("✅ Todos los datos insertados correctamente en CLEAN.ORDER_PRODUCTS_CLEAN.")

    except Exception as e:
        raise Exception(f"❌ Error al guardar en CLEAN.ORDER_PRODUCTS_CLEAN: {e}")

    return order_products_df


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Test para validar la limpieza de datos en ORDER_PRODUCTS_CLEAN.
    """
    assert output is not None, '❌ El output está vacío'
    assert isinstance(output, pd.DataFrame), '❌ El output no es un DataFrame'

    assert 'ORDER_ID' in output.columns, '❌ ORDER_ID no está en las columnas'
    assert 'PRODUCT_ID' in output.columns, '❌ PRODUCT_ID no está en las columnas'
    assert 'ADD_TO_CART_ORDER' in output.columns, '❌ ADD_TO_CART_ORDER no está en las columnas'
    assert 'REORDERED' in output.columns, '❌ REORDERED no está en las columnas'

    assert output['ORDER_ID'].dtype in ['int64', 'Int64'], '❌ ORDER_ID no tiene el tipo correcto'
    assert output['PRODUCT_ID'].dtype in ['int64', 'Int64'], '❌ PRODUCT_ID no tiene el tipo correcto'
    assert output['ADD_TO_CART_ORDER'].dtype == 'float64', '❌ ADD_TO_CART_ORDER no tiene el tipo correcto'
    assert output['REORDERED'].dtype in ['int64', 'Int64'], '❌ REORDERED no tiene el tipo correcto'

    print("✅ Todos los tests pasaron correctamente.")
