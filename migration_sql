import pyodbc
import pandas as pd

# Función para conectar a la base de datos
def conectar_bd(dsn, usuario, contrasena, base_datos):
    try:
        conexion = pyodbc.connect(f'DSN={dsn};UID={usuario};PWD={contrasena};DATABASE={base_datos}')
        print(f"Conexión exitosa a {base_datos}")
        return conexion
    except pyodbc.Error as e:
        print(f"Error al conectar a la base de datos {base_datos}: {e}")
        return None

# Función para obtener las tablas de una base de datos
def obtener_tablas(conexion):
    try:
        query = "SELECT table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE'"
        tablas = pd.read_sql(query, conexion)
        return tablas['table_name'].tolist()
    except Exception as e:
        print(f"Error al obtener tablas: {e}")
        return []

# Función para comparar esquemas de dos tablas en diferentes bases de datos
def comparar_esquemas(tabla, esquema_1, esquema_2):
    try:
        if esquema_1.shape != esquema_2.shape:
            print(f"Esquema diferente para la tabla {tabla}: Diferente número de columnas o tipos.")
        else:
            if not esquema_1.equals(esquema_2):
                print(f"Esquema diferente para la tabla {tabla}: Columnas o tipos de datos no coinciden.")
            else:
                print(f"Esquema correcto para la tabla {tabla}.")
    except Exception as e:
        print(f"Error al comparar esquemas para la tabla {tabla}: {e}")

# Comparar cantidad de registros entre dos bases de datos
def comparar_datos_agrupados(conexion_1, conexion_2, tabla):
    try:
        query = f"SELECT COUNT(*) AS total FROM {tabla}"
        total_1 = pd.read_sql(query, conexion_1).iloc[0]['total']
        total_2 = pd.read_sql(query, conexion_2).iloc[0]['total']
        
        if total_1 != total_2:
            print(f"Diferente número de filas en la tabla {tabla}: {total_1} vs {total_2}")
        else:
            print(f"Los totales coinciden para la tabla {tabla}: {total_1} filas.")
    except Exception as e:
        print(f"Error al comparar el número de filas para la tabla {tabla}: {e}")

# Comparar los datos fila a fila
def comparar_filas(conexion_1, conexion_2, tabla):
    try:
        query = f"SELECT * FROM {tabla}"
        df_1 = pd.read_sql(query, conexion_1)
        df_2 = pd.read_sql(query, conexion_2)
        
        # Comparar si los DataFrames son iguales
        if not df_1.equals(df_2):
            print(f"Diferencia de datos en la tabla {tabla}: Las filas no coinciden.")
        else:
            print(f"Los datos de la tabla {tabla} son iguales.")
    except Exception as e:
        print(f"Error al comparar los datos fila a fila para la tabla {tabla}: {e}")

# Prueba de datos nulos
def prueba_datos_nulos(conexion_1, conexion_2, tabla, columnas):
    for columna in columnas:
        try:
            query_1 = f"SELECT COUNT(*) AS nulos FROM {tabla} WHERE {columna} IS NULL"
            query_2 = f"SELECT COUNT(*) AS nulos FROM {tabla} WHERE {columna} IS NULL"
            
            nulos_1 = pd.read_sql(query_1, conexion_1).iloc[0]['nulos']
            nulos_2 = pd.read_sql(query_2, conexion_2).iloc[0]['nulos']

            if nulos_1 != nulos_2:
                print(f"Diferente cantidad de valores nulos en la columna {columna} de la tabla {tabla}: {nulos_1} vs {nulos_2}")
            else:
                print(f"Los valores nulos coinciden para la columna {columna} en la tabla {tabla}: {nulos_1}")
        except Exception as e:
            print(f"Error al verificar valores nulos en la columna {columna} de la tabla {tabla}: {e}")

# Prueba de valores únicos
def prueba_unicidad(conexion_1, conexion_2, tabla, columna):
    try:
        query_1 = f"SELECT COUNT(DISTINCT {columna}) AS unique_count FROM {tabla}"
        query_2 = f"SELECT COUNT(DISTINCT {columna}) AS unique_count FROM {tabla}"

        unique_count_1 = pd.read_sql(query_1, conexion_1).iloc[0]['unique_count']
        unique_count_2 = pd.read_sql(query_2, conexion_2).iloc[0]['unique_count']

        if unique_count_1 != unique_count_2:
            print(f"Diferente cantidad de valores únicos en la columna {columna} de la tabla {tabla}: {unique_count_1} vs {unique_count_2}")
        else:
            print(f"Los valores únicos coinciden para la columna {columna} en la tabla {tabla}: {unique_count_1}")
    except Exception as e:
        print(f"Error al verificar valores únicos en la columna {columna} de la tabla {tabla}: {e}")

# Prueba de rangos para una columna numérica
def prueba_rango(conexion_1, conexion_2, tabla, columna, min_valor, max_valor):
    try:
        query_1 = f"SELECT MIN({columna}) AS min, MAX({columna}) AS max FROM {tabla}"
        query_2 = f"SELECT MIN({columna}) AS min, MAX({columna}) AS max FROM {tabla}"

        min_max_1 = pd.read_sql(query_1, conexion_1)
        min_max_2 = pd.read_sql(query_2, conexion_2)

        min_1, max_1 = min_max_1.iloc[0]['min'], min_max_1.iloc[0]['max']
        min_2, max_2 = min_max_2.iloc[0]['min'], min_max_2.iloc[0]['max']

        if min_1 < min_valor or max_1 > max_valor:
            print(f"Rango fuera de límites en la columna {columna} de la tabla {tabla} en la base de datos 1.")
        if min_2 < min_valor or max_2 > max_valor:
            print(f"Rango fuera de límites en la columna {columna} de la tabla {tabla} en la base de datos 2.")
    except Exception as e:
        print(f"Error al verificar el rango de la columna {columna} en la tabla {tabla}: {e}")

# Comparar fechas (ejemplo: fecha de nacimiento no puede ser futura)
def prueba_fechas(conexion_1, conexion_2, tabla, columna_fecha):
    try:
        query_1 = f"SELECT COUNT(*) AS invalid_dates FROM {tabla} WHERE {columna_fecha} > GETDATE()"
        query_2 = f"SELECT COUNT(*) AS invalid_dates FROM {tabla} WHERE {columna_fecha} > GETDATE()"

        invalid_dates_1 = pd.read_sql(query_1, conexion_1).iloc[0]['invalid_dates']
        invalid_dates_2 = pd.read_sql(query_2, conexion_2).iloc[0]['invalid_dates']

        if invalid_dates_1 > 0:
            print(f"Fechas inválidas en la columna {columna_fecha} de la tabla {tabla} en la base de datos 1.")
        if invalid_dates_2 > 0:
            print(f"Fechas inválidas en la columna {columna_fecha} de la tabla {tabla} en la base de datos 2.")
    except Exception as e:
        print(f"Error al verificar fechas en la columna {columna_fecha} de la tabla {tabla}: {e}")

# Función principal para ejecutar las validaciones
def validar_migracion(dsn_1, dsn_2, usuario, contrasena, base_datos_1, base_datos_2):
    try:
        conexion_1 = conectar_bd(dsn_1, usuario, contrasena, base_datos_1)
        conexion_2 = conectar_bd(dsn_2, usuario, contrasena, base_datos_2)

        if not conexion_1 or not conexion_2:
            print("No se pudo establecer conexión con una de las bases de datos. Abortando.")
            return
        
        tablas_1 = obtener_tablas(conexion_1)
        tablas_2 = obtener_tablas(conexion_2)

        if sorted(tablas_1) != sorted(tablas_2):
            print("Las bases de datos tienen tablas diferentes.")
            return

        for tabla in tablas_1:
            print(f"\nVerificando tabla {tabla}...")

            # Comparar esquemas
            esquema_1 = pd.read
