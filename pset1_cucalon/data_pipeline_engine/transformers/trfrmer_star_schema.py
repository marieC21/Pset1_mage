if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import snowflake.connector

@transformer
def transform(order_products_df: pd.DataFrame, 
              instacart_orders_df: pd.DataFrame,
              products_df: pd.DataFrame,
              aisles_df: pd.DataFrame,
              departments_df: pd.DataFrame,
              *args, **kwargs) -> dict:
    """
    Transformer para construir y cargar el Star Schema en Snowflake CLEAN.
    """

    print("🔄 Creando el Star Schema en Snowflake CLEAN...")

    # ==============================
    # 🔹 Validación de Columnas
    # ==============================
    print("📌 Columnas antes de la limpieza:")
    print("🔹 PRODUCTS:", products_df.columns)
    print("🔹 AISLES:", aisles_df.columns)
    print("🔹 DEPARTMENTS:", departments_df.columns)

    # Convertir todos los nombres de columnas a minúsculas
    products_df.columns = products_df.columns.str.lower()
    aisles_df.columns = aisles_df.columns.str.lower()
    departments_df.columns = departments_df.columns.str.lower()

    # Validar nombres de columnas esperados
    expected_products = {'product_id', 'product_name', 'aisle_id', 'department_id'}
    expected_aisles = {'aisle_id', 'aisle'}
    expected_departments = {'department_id', 'department'}

    missing_products = expected_products - set(products_df.columns)
    missing_aisles = expected_aisles - set(aisles_df.columns)
    missing_departments = expected_departments - set(departments_df.columns)

    if missing_products:
        raise KeyError(f"❌ La tabla PRODUCTS no tiene las columnas necesarias: {missing_products}")
    if missing_aisles:
        raise KeyError(f"❌ La tabla AISLES no tiene las columnas necesarias: {missing_aisles}")
    if missing_departments:
        raise KeyError(f"❌ La tabla DEPARTMENTS no tiene las columnas necesarias: {missing_departments}")

    # ==============================
    # 🔹 CREAR TABLA DE HECHOS
    # ==============================
    print("🔹 Uniendo ORDER_PRODUCTS con INSTACART_ORDERS para FACT_ORDERS...")

    fact_orders_df = order_products_df.merge(instacart_orders_df, on='order_id', how='inner')

    print(f"✅ Tabla de hechos FACT_ORDERS creada con {fact_orders_df.shape[0]} filas y {fact_orders_df.shape[1]} columnas.")

    # ==============================
    # 🔹 CREAR TABLA DE DIMENSIONES
    # ==============================
    print("🔹 Uniendo PRODUCTS con AISLES y DEPARTMENTS para DIM_PRODUCTS...")

    dim_products_df = products_df.merge(aisles_df, on='aisle_id', how='left') \
                                 .merge(departments_df, on='department_id', how='left')

    print(f"✅ Tabla de dimensiones DIM_PRODUCTS creada con {dim_products_df.shape[0]} filas y {dim_products_df.shape[1]} columnas.")

    # ==============================
    # 🔹 CARGAR A SNOWFLAKE CLEAN
    # ==============================
    print("🔄 Conectando a Snowflake CLEAN para cargar los datos...")

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

        # Subir FACT_ORDERS
        print("⬆️ Cargando FACT_ORDERS en Snowflake CLEAN...")
        cursor.execute("CREATE OR REPLACE TABLE CLEAN.FACT_ORDERS AS SELECT * FROM values " +
                       ",".join(str(tuple(row)) for row in fact_orders_df.to_records(index=False)))
        print("✅ FACT_ORDERS cargada exitosamente.")

        # Subir DIM_PRODUCTS
        print("⬆️ Cargando DIM_PRODUCTS en Snowflake CLEAN...")
        cursor.execute("CREATE OR REPLACE TABLE CLEAN.DIM_PRODUCTS AS SELECT * FROM values " +
                       ",".join(str(tuple(row)) for row in dim_products_df.to_records(index=False)))
        print("✅ DIM_PRODUCTS cargada exitosamente.")

        # Cerrar conexión
        conn.close()
        print("🔌 Conexión a Snowflake cerrada.")

    except Exception as e:
        raise Exception(f"❌ Error al cargar en Snowflake CLEAN: {e}")

    print("🎉 Star Schema creado y cargado exitosamente en Snowflake CLEAN.")

    return {
        "fact_orders": fact_orders_df,
        "dim_products": dim_products_df
    }
