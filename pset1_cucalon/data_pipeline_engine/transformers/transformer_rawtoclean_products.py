if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import snowflake.connector

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Transformer para limpiar la tabla 'products' y guardarla en CLEAN.PRODUCTS_CLEAN en Snowflake.
    """
    print("🔄 Conectando a Snowflake desde el Transformer...")

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
        
        # 📌 Extraer los datos de RAW.PRODUCTS con comillas dobles para evitar errores de identificador
        query = 'SELECT "product_id", "product_name", "aisle_id", "department_id" FROM RAW.PRODUCTS;'
        products_df = pd.read_sql(query, conn)

        print("📌 Datos extraídos de RAW.PRODUCTS")

    except Exception as e:
        raise Exception(f"❌ Error al conectar a Snowflake: {e}")

    # Convertir nombres de columnas a minúsculas para evitar errores
    products_df.columns = products_df.columns.str.lower()

    # Verificar qué columnas están disponibles
    print("📌 Columnas disponibles en PRODUCTS:", products_df.columns.tolist())

    # Validar que las columnas necesarias existen
    required_columns = {'product_id', 'product_name', 'aisle_id', 'department_id'}
    missing_columns = required_columns - set(products_df.columns)
    if missing_columns:
        raise KeyError(f"❌ Faltan las siguientes columnas en PRODUCTS: {missing_columns}")

    # Convertir tipos de datos (VARCHAR a INT)
    for col in ['product_id', 'aisle_id', 'department_id']:
        products_df[col] = pd.to_numeric(products_df[col], errors='coerce').astype('Int64')

    # Reemplazar valores nulos en product_name
    products_df['product_name'] = products_df['product_name'].fillna('No registrado')

    # 🔄 Guardar la versión limpia en CLEAN.PRODUCTS_CLEAN
    try:
        conn.cursor().execute("USE SCHEMA CLEAN;")  # Cambiar a CLEAN schema
        print("📌 Usando schema CLEAN")

        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS CLEAN.PRODUCTS_CLEAN (
            product_id INT,
            product_name STRING,
            aisle_id INT,
            department_id INT
        );
        """
        conn.cursor().execute(create_table_query)
        print("✅ Tabla PRODUCTS_CLEAN creada/verificada en CLEAN")

        # Insertar datos en la tabla CLEAN
        insert_query = "INSERT INTO CLEAN.PRODUCTS_CLEAN (product_id, product_name, aisle_id, department_id) VALUES (%s, %s, %s, %s)"
        conn.cursor().executemany(insert_query, products_df.values.tolist())
        print(f"✅ {len(products_df)} filas insertadas en CLEAN.PRODUCTS_CLEAN")

    except Exception as e:
        raise Exception(f"❌ Error al guardar en CLEAN.PRODUCTS_CLEAN: {e}")

    finally:
        # Cerrar conexión
        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    # 📌 Retornar el DataFrame limpio
    print("📌 Transformación completada. Retornando datos limpios.")
    return products_df


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Test para validar la limpieza de datos.
    """
    assert output is not None, '❌ El output está vacío'
    assert isinstance(output, pd.DataFrame), '❌ El output no es un DataFrame'
    assert 'product_id' in output.columns, '❌ product_id no está en las columnas'
    assert output['product_id'].dtype in ['int64', 'Int64'], '❌ product_id no tiene el tipo correcto'
    assert output['aisle_id'].dtype in ['int64', 'Int64'], '❌ aisle_id no tiene el tipo correcto'
    assert output['department_id'].dtype in ['int64', 'Int64'], '❌ department_id no tiene el tipo correcto'
    assert output['product_name'].isnull().sum() == 0, '❌ Hay valores nulos en product_name'

