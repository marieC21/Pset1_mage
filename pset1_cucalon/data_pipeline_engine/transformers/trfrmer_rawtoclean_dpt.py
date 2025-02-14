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
    Transformer para limpiar la tabla 'DEPARTMENTS' y guardarla en CLEAN.DEPARTMENTS_CLEAN.
    """
    print("🔄 Conectando a Snowflake para extraer datos de DEPARTMENTS...")

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

        # Extraer los datos de RAW.DEPARTMENTS
        query = 'SELECT "department_id", "department" FROM RAW.DEPARTMENTS;'
        departments_df = pd.read_sql(query, conn)

        print("📌 Datos extraídos de RAW.DEPARTMENTS")

    except Exception as e:
        raise Exception(f"❌ Error al conectar a Snowflake: {e}")

    # Convertir nombres de columnas a minúscula
    departments_df.columns = departments_df.columns.str.lower()

    # Validar columnas
    required_columns = {'department_id', 'department'}
    missing_columns = required_columns - set(departments_df.columns)
    if missing_columns:
        raise KeyError(f"❌ Faltan las siguientes columnas en DEPARTMENTS: {missing_columns}")

    # Convertir tipos de datos
    departments_df['department_id'] = pd.to_numeric(departments_df['department_id'], errors='coerce').astype('Int64')
    departments_df['department'] = departments_df['department'].astype(str)  # Mantener como string

    # 🔄 Guardar la versión limpia en CLEAN.DEPARTMENTS_CLEAN por lotes
    try:
        conn.cursor().execute("USE SCHEMA CLEAN;")  # Cambiar a CLEAN schema
        print("📌 Usando schema CLEAN")

        # Crear tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS CLEAN.DEPARTMENTS_CLEAN (
            department_id INT,
            department STRING
        );
        """
        conn.cursor().execute(create_table_query)
        print("✅ Tabla DEPARTMENTS_CLEAN creada/verificada en CLEAN")

        # Dividir el DataFrame en lotes de 50,000 filas para evitar el error
        batch_size = 50000
        num_batches = int(np.ceil(len(departments_df) / batch_size))

        print(f"🔄 Insertando datos en lotes de {batch_size} filas ({num_batches} lotes en total)...")

        insert_query = """
        INSERT INTO CLEAN.DEPARTMENTS_CLEAN (department_id, department) 
        VALUES (%s, %s)
        """

        cursor = conn.cursor()
        for i in range(num_batches):
            batch = departments_df.iloc[i * batch_size : (i + 1) * batch_size].values.tolist()
            cursor.executemany(insert_query, batch)
            print(f"✅ Lote {i+1}/{num_batches} insertado con {len(batch)} filas")

        print("✅ Todos los lotes insertados correctamente en CLEAN.DEPARTMENTS_CLEAN")

    except Exception as e:
        raise Exception(f"❌ Error al guardar en CLEAN.DEPARTMENTS_CLEAN: {e}")

    finally:
        # Cerrar conexión
        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    return departments_df


@test
def test_output(output: pd.DataFrame, *args) -> None:
    """
    Test para validar la limpieza de datos de DEPARTMENTS.
    """
    assert output is not None, '❌ El output está vacío'
    assert isinstance(output, pd.DataFrame), '❌ El output no es un DataFrame'
    assert 'department_id' in output.columns, '❌ department_id no está en las columnas'
    assert output['department_id'].dtype in ['int64', 'Int64'], '❌ department_id no tiene el tipo correcto'
    assert output['department'].dtype == 'object', '❌ department no tiene el tipo correcto (debe ser texto)'
