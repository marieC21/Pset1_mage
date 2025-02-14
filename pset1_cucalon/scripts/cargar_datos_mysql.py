import pandas as pd
import mysql.connector

# Configuración de conexión a MySQL
conexion = mysql.connector.connect(
    host="localhost",
    user="root",
    password="MarieC2001@",
    database="pset1_db",
    autocommit=True
)

print("✅ Conexión a MySQL establecida.")

# ✅ Rutas de los archivos CSV
archivos_csv = {
    "aisles": "/Users/chants/Desktop/DataMining/pset1_cucalon/data/aisles.csv",
    "departments": "/Users/chants/Desktop/DataMining/pset1_cucalon/data/departments.csv",
    "products": "/Users/chants/Desktop/DataMining/pset1_cucalon/data/products.csv",
    "instacart_orders": "/Users/chants/Desktop/DataMining/pset1_cucalon/data/instacart_orders.csv",
    "order_products": "/Users/chants/Desktop/DataMining/pset1_cucalon/data/order_products.csv"
}

cursor = conexion.cursor()

# 🔹 Deshabilitar restricciones de claves foráneas antes de eliminar tablas
cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
print("⚠️ Restricciones de claves foráneas deshabilitadas.")

# 🔹 Eliminar todas las tablas si existen
for tabla in archivos_csv.keys():
    cursor.execute(f"DROP TABLE IF EXISTS {tabla}")
    print(f"🗑️ Tabla {tabla} eliminada (si existía).")

# 🔹 Habilitar nuevamente las restricciones de claves foráneas
cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
print("✅ Restricciones de claves foráneas habilitadas nuevamente.")

# 🔹 Leer archivos CSV sin modificar tipos de datos
dataframes = {tabla: pd.read_csv(ruta, sep=";", dtype=str) for tabla, ruta in archivos_csv.items()}

print("📂 Archivos CSV cargados correctamente.")

# 🔹 Crear y cargar todas las tablas excepto `order_products`
for nombre_tabla, df in dataframes.items():
    if nombre_tabla == "order_products":
        continue  # Saltamos order_products, se maneja aparte

    columnas = df.columns.tolist()
    
    # Crear tabla con todas las columnas con los mismos nombres
    columnas_sql = ", ".join([f"`{col}` TEXT" for col in columnas])
    query_crear_tabla = f"CREATE TABLE {nombre_tabla} ({columnas_sql})"
    cursor.execute(query_crear_tabla)
    print(f"🆕 Tabla {nombre_tabla} creada.")

    # 🔹 Insertar datos en MySQL
    valores_placeholder = ", ".join(["%s"] * len(columnas))
    query_insertar = f"INSERT INTO {nombre_tabla} VALUES ({valores_placeholder})"

    cursor.executemany(query_insertar, df.values.tolist())
    conexion.commit()  # Confirmar inserción

    print(f"✅ Carga completada: {nombre_tabla}")

# 🔹 Manejo separado de `order_products`
df_order_products = dataframes["order_products"]

# 🔹 Crear la tabla `order_products` antes de insertar los datos
columnas_order_products = df_order_products.columns.tolist()

# Crear tabla con los mismos nombres de columnas sin modificar tipos de datos
columnas_sql_order = ", ".join([f"`{col}` TEXT" for col in columnas_order_products])
query_crear_order_products = f"CREATE TABLE order_products ({columnas_sql_order})"
cursor.execute(query_crear_order_products)
print("🆕 Tabla order_products creada correctamente.")

# 🔹 Insertar los datos en lotes para evitar desconexiones
query_order_products = f"INSERT IGNORE INTO order_products VALUES ({', '.join(['%s'] * len(columnas_order_products))})"

data_list = df_order_products.values.tolist()
batch_size = 10000  # Inserta en lotes de 10,000 filas

for i in range(0, len(data_list), batch_size):
    batch = data_list[i:i + batch_size]
    cursor.executemany(query_order_products, batch)
    conexion.commit()  # Confirmar después de cada lote
    print(f"✅ Insertadas {i + len(batch)} filas en order_products...")

cursor.close()
print("✅ Carga completada: order_products")

print("✅ Todos los datos fueron insertados correctamente en MySQL.")

# Cerrar la conexión
conexion.close()
print("🔒 Conexión cerrada.")









