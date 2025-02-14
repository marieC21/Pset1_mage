if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import snowflake.connector

@transformer
def transform(*args, **kwargs) -> pd.DataFrame:
    """
    Transformer para construir la tabla DIM_PRODUCTS combinando las tablas limpias en CLEAN.
    """

    print("🔄 Conectando a Snowflake para extraer datos limpios de CLEAN...")

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
        products_df = pd.read_sql("SELECT * FROM CLEAN.PRODUCTS_CLEAN;", conn)
        aisles_df = pd.read_sql("SELECT * FROM CLEAN.AISLES_CLEAN;", conn)
        departments_df = pd.read_sql("SELECT * FROM CLEAN.DEPARTMENTS_CLEAN;", conn)

        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    except Exception as e:
        raise Exception(f"❌ Error al conectar a Snowflake: {e}")

    # 🔄 Unir las tablas correctamente asegurando que los tipos son correctos
    print("🔄 Uniendo PRODUCTS_CLEAN con AISLES_CLEAN y DEPARTMENTS_CLEAN...")
    dim_products_df = (
        products_df
        .merge(aisles_df, on='AISLE_ID', how='left')
        .merge(departments_df, on='DEPARTMENT_ID', how='left')
    )

    # Verificar si hay valores incorrectos
    invalid_rows = dim_products_df[
        ~dim_products_df['PRODUCT_ID'].astype(str).str.isnumeric() |
        ~dim_products_df['AISLE_ID'].astype(str).str.isnumeric() |
        ~dim_products_df['DEPARTMENT_ID'].astype(str).str.isnumeric()
    ]

    if not invalid_rows.empty:
        print("❌ Filas con valores incorrectos (PRODUCT_ID, AISLE_ID, DEPARTMENT_ID):")
        print(invalid_rows)
        # Filtrar solo registros válidos
        dim_products_df = dim_products_df[
            dim_products_df['PRODUCT_ID'].astype(str).str.isnumeric() &
            dim_products_df['AISLE_ID'].astype(str).str.isnumeric() &
            dim_products_df['DEPARTMENT_ID'].astype(str).str.isnumeric()
        ]
        print("✅ Filas incorrectas eliminadas.")

    # Convertir tipos de datos correctamente
    dim_products_df['PRODUCT_ID'] = dim_products_df['PRODUCT_ID'].astype(int)
    dim_products_df['AISLE_ID'] = dim_products_df['AISLE_ID'].astype(int)
    dim_products_df['DEPARTMENT_ID'] = dim_products_df['DEPARTMENT_ID'].astype(int)
    dim_products_df['PRODUCT_NAME'] = dim_products_df['PRODUCT_NAME'].astype(str)
    dim_products_df['AISLE'] = dim_products_df['AISLE'].astype(str)
    dim_products_df['DEPARTMENT'] = dim_products_df['DEPARTMENT'].astype(str)

    print("✅ DIM_PRODUCTS creada con", dim_products_df.shape[0], "filas y", dim_products_df.shape[1], "columnas.")

    # Guardar en Snowflake en lotes de 5000 filas
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

        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS CLEAN.DIM_PRODUCTS (
            PRODUCT_ID INT,
            PRODUCT_NAME VARCHAR(255),
            AISLE_ID INT,
            AISLE VARCHAR(255),
            DEPARTMENT_ID INT,
            DEPARTMENT VARCHAR(255)
        );
        """
        cursor.execute(create_table_query)
        print("✅ Tabla DIM_PRODUCTS creada/verificada en CLEAN.")

        # Insertar datos en Snowflake en lotes de 5000 filas
        print("🔄 Insertando datos en lotes de 5000 filas...")
        insert_query = """
        INSERT INTO CLEAN.DIM_PRODUCTS (PRODUCT_ID, PRODUCT_NAME, AISLE_ID, AISLE, DEPARTMENT_ID, DEPARTMENT)
        VALUES (%s, %s, %s, %s, %s, %s);
        """
        batch_size = 5000
        for i in range(0, len(dim_products_df), batch_size):
            batch = dim_products_df.iloc[i:i+batch_size].values.tolist()
            cursor.executemany(insert_query, batch)
            conn.commit()
            print(f"✅ Insertadas {len(batch)} filas...")

        cursor.close()
        conn.close()
        print("✅ Datos insertados correctamente en CLEAN.DIM_PRODUCTS.")

    except Exception as e:
        raise Exception(f"❌ Error al guardar en CLEAN.DIM_PRODUCTS: {e}")

    return dim_products_df


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Test para validar la integridad de la tabla DIM_PRODUCTS.
    """
    assert output is not None, '❌ El output está vacío'
    assert isinstance(output, pd.DataFrame), '❌ El output no es un DataFrame'
    
    expected_columns = ['PRODUCT_ID', 'PRODUCT_NAME', 'AISLE_ID', 'AISLE', 'DEPARTMENT_ID', 'DEPARTMENT']
    
    for col in expected_columns:
        assert col in output.columns, f'❌ {col} no está en las columnas'

    assert output['PRODUCT_ID'].dtype in ['int64', 'Int64'], '❌ PRODUCT_ID no tiene el tipo correcto'
    assert output['AISLE_ID'].dtype in ['int64', 'Int64'], '❌ AISLE_ID no tiene el tipo correcto'
    assert output['DEPARTMENT_ID'].dtype in ['int64', 'Int64'], '❌ DEPARTMENT_ID no tiene el tipo correcto'
    assert output['PRODUCT_NAME'].dtype == 'object', '❌ PRODUCT_NAME debe ser texto'
    assert output['AISLE'].dtype == 'object', '❌ AISLE debe ser texto'
    assert output['DEPARTMENT'].dtype == 'object', '❌ DEPARTMENT debe ser texto'

    assert output['AISLE'].isnull().sum() == 0, '❌ Hay valores nulos en AISLE'
    assert output['DEPARTMENT'].isnull().sum() == 0, '❌ Hay valores nulos en DEPARTMENT'

    print("✅ Todos los tests pasaron correctamente.")
