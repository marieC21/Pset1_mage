if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import snowflake.connector

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Transformer para crear la tabla de hechos FACT_ORDERS combinando 
    INSTACART_ORDERS_CLEAN y ORDER_PRODUCTS_CLEAN.
    """

    print("🔄 Conectando a Snowflake para extraer datos...")

    # Conectar a Snowflake
    try:
        conn = snowflake.connector.connect(
            user="SERVICE_USER",
            password="BF2001$$ash",
            account="ADAPFIL-NRB91509",
            warehouse="COMPUTE_WH",
            database="RAPIDB",
            schema="CLEAN"
        )
        print("✅ Conexión exitosa a Snowflake")

        # Extraer datos
        orders_query = "SELECT * FROM CLEAN.INSTACART_ORDERS_CLEAN;"
        orders_df = pd.read_sql(orders_query, conn)

        order_products_query = "SELECT * FROM CLEAN.ORDER_PRODUCTS_CLEAN;"
        order_products_df = pd.read_sql(order_products_query, conn)

        print("🔌 Conexión a Snowflake cerrada.")
        conn.close()

    except Exception as e:
        raise Exception(f"❌ Error al conectar a Snowflake: {e}")

    # Convertir nombres de columnas a mayúsculas
    orders_df.columns = orders_df.columns.str.upper()
    order_products_df.columns = order_products_df.columns.str.upper()

    # Verificar qué columnas están disponibles
    print("📌 Columnas en INSTACART_ORDERS_CLEAN:", orders_df.columns.tolist())
    print("📌 Columnas en ORDER_PRODUCTS_CLEAN:", order_products_df.columns.tolist())

    # Validar que las columnas necesarias existen
    required_orders_columns = {'ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY', 'DAYS_SINCE_PRIOR_ORDER'}
    required_order_products_columns = {'ORDER_ID', 'PRODUCT_ID', 'ADD_TO_CART_ORDER', 'REORDERED'}

    missing_orders = required_orders_columns - set(orders_df.columns)
    missing_order_products = required_order_products_columns - set(order_products_df.columns)

    if missing_orders:
        raise KeyError(f"❌ Faltan columnas en INSTACART_ORDERS_CLEAN: {missing_orders}")
    if missing_order_products:
        raise KeyError(f"❌ Faltan columnas en ORDER_PRODUCTS_CLEAN: {missing_order_products}")

    # 🔄 Unir tablas en FACT_ORDERS
    fact_orders_df = order_products_df.merge(orders_df, on="ORDER_ID", how="inner")

    # Reordenar columnas para que mantengan la estructura deseada
    fact_orders_df = fact_orders_df[['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 
                                     'ORDER_HOUR_OF_DAY', 'DAYS_SINCE_PRIOR_ORDER', 
                                     'PRODUCT_ID', 'ADD_TO_CART_ORDER', 'REORDERED']]
    
    print(f"✅ FACT_ORDERS creada con {fact_orders_df.shape[0]} filas y {fact_orders_df.shape[1]} columnas.")

    # Guardar en Snowflake en chunks
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
        create_table_query = """
        CREATE TABLE IF NOT EXISTS CLEAN.FACT_ORDERS (
            ORDER_ID INT,
            USER_ID INT,
            ORDER_NUMBER INT,
            ORDER_DOW INT,
            ORDER_HOUR_OF_DAY INT,
            DAYS_SINCE_PRIOR_ORDER FLOAT,
            PRODUCT_ID INT,
            ADD_TO_CART_ORDER FLOAT,
            REORDERED INT
        );
        """
        cursor.execute(create_table_query)
        print("✅ Tabla FACT_ORDERS creada/verificada en CLEAN.")

        # Insertar datos en chunks
        print("🔄 Insertando datos en CLEAN.FACT_ORDERS en lotes de 50,000...")
        chunk_size = 50000
        for i in range(0, fact_orders_df.shape[0], chunk_size):
            chunk = fact_orders_df.iloc[i:i+chunk_size]
            values = ', '.join([f"({row.ORDER_ID}, {row.USER_ID}, {row.ORDER_NUMBER}, {row.ORDER_DOW}, {row.ORDER_HOUR_OF_DAY}, {row.DAYS_SINCE_PRIOR_ORDER}, {row.PRODUCT_ID}, {row.ADD_TO_CART_ORDER}, {row.REORDERED})" 
                                for _, row in chunk.iterrows()])
            insert_query = f"""
            INSERT INTO CLEAN.FACT_ORDERS (ORDER_ID, USER_ID, ORDER_NUMBER, ORDER_DOW, ORDER_HOUR_OF_DAY, DAYS_SINCE_PRIOR_ORDER, PRODUCT_ID, ADD_TO_CART_ORDER, REORDERED)
            VALUES {values};
            """
            cursor.execute(insert_query)
            print(f"✅ Insertados {len(chunk)} registros.")

        conn.commit()
        print("✅ Datos insertados correctamente en CLEAN.FACT_ORDERS.")

        # Cerrar conexión
        cursor.close()
        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    except Exception as e:
        raise Exception(f"❌ Error al guardar en CLEAN.FACT_ORDERS: {e}")

    return fact_orders_df


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Test para validar la integridad de la tabla de hechos FACT_ORDERS.
    """
    assert output is not None, '❌ El output está vacío'
    assert isinstance(output, pd.DataFrame), '❌ El output no es un DataFrame'
    
    expected_columns = ['ORDER_ID', 'USER_ID', 'ORDER_NUMBER', 'ORDER_DOW', 'ORDER_HOUR_OF_DAY', 
                        'DAYS_SINCE_PRIOR_ORDER', 'PRODUCT_ID', 'ADD_TO_CART_ORDER', 'REORDERED']
    
    for col in expected_columns:
        assert col in output.columns, f'❌ {col} no está en las columnas'

    assert output['ORDER_ID'].dtype in ['int64', 'Int64'], '❌ ORDER_ID no tiene el tipo correcto'
    assert output['USER_ID'].dtype in ['int64', 'Int64'], '❌ USER_ID no tiene el tipo correcto'
    assert output['ORDER_NUMBER'].dtype in ['int64', 'Int64'], '❌ ORDER_NUMBER no tiene el tipo correcto'
    assert output['ORDER_DOW'].dtype in ['int64', 'Int64'], '❌ ORDER_DOW no tiene el tipo correcto'
    assert output['ORDER_HOUR_OF_DAY'].dtype in ['int64', 'Int64'], '❌ ORDER_HOUR_OF_DAY no tiene el tipo correcto'
    assert output['DAYS_SINCE_PRIOR_ORDER'].dtype == 'float64', '❌ DAYS_SINCE_PRIOR_ORDER no tiene el tipo correcto'
    assert output['PRODUCT_ID'].dtype in ['int64', 'Int64'], '❌ PRODUCT_ID no tiene el tipo correcto'
    assert output['ADD_TO_CART_ORDER'].dtype == 'float64', '❌ ADD_TO_CART_ORDER no tiene el tipo correcto'
    assert output['REORDERED'].dtype in ['int64', 'Int64'], '❌ REORDERED no tiene el tipo correcto'

    print("✅ Todos los tests pasaron correctamente.")
