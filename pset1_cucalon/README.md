# Proyecto de Análisis de Datos - Star Schema

Este proyecto implementa un modelo dimensional con Snowflake y Mage AI para análisis de datos sobre órdenes de compra.

## 📌 Estructura del Proyecto
- `insights.ipynb` → Notebook con análisis exploratorio y respuestas a las preguntas clave.
- `PRODUCTS_EDA.ipynb` → EDA de la tabla Products desde Snowflake
- `AISLE_EDA.ipynb` → EDA de la tabla Aisle desde Snowflake
- `DPT_EDA.ipynb` → EDA de la tabla Departments desde Snowflake
- `INSTACART_EDA.ipynb` → EDA de la tabla Instacart_Orders desde Snowflake
- `OORDERPROD_EDA.ipynb` → EDA de la tabla Order_products desde Snowflake
- `requirements.txt` → Lista de dependencias necesarias para ejecutar el proyecto.
- `README.md` → Documentación del proyecto.
- `Data` → Carpeta con los datos csv.
- `carcar_datos_mysql.py` → Código para cargar cdv a mysql.
- `data_pipeline_engine` → Documentación de mageai

## Preguntas Respondidas (insight)
1. Comportamiento de compra según día de la semana
2. Comportamiento de compra según hora del día
3. Comportamiento según hora del día y día de la semana
4. Distribución de las órdenes hechas por los clientes
5. Top 20 productos más frecuentes
6. ¿Cuántos artículos se compran generalmente en un pedido?
7. Top 20 artículos que se vuelven a pedir con más frecuencia
8. Proporción de pedidos que se vuelven a pedir para cada producto
9. Proporción de productos pedidos que se vuelven a pedir para cada cliente
10. Top 20 artículos que la gente pone primero en el carrito

