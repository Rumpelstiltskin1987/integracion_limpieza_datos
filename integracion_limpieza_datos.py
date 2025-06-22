import pandas as pd

# --- 1. Cargar ambos archivos CSV en DataFrames de pandas ---
# Usamos un bloque try-except para manejar elegantemente los casos en que los archivos no se encuentren.
try:
    ventas_df = pd.read_csv('ventas.csv')
    clientes_df = pd.read_csv('clientes.csv')
    print("Archivos CSV cargados exitosamente.")
except FileNotFoundError:
    print("Error: Asegúrate de que 'ventas.csv' y 'clientes.csv' estén en el mismo directorio que el script.")
    # Si los archivos no se encuentran, no hay datos que procesar, por lo que salimos.
    exit()

# Mostrar información inicial para obtener una visión general de los DataFrames.
print("\n--- Información Inicial de 'ventas_df' ---")
ventas_df.info()
print("\n--- Información Inicial de 'clientes_df' ---")
clientes_df.info()

# --- 2. Realizar un merge interno usando 'cliente_id' ---
# Un 'inner merge' combina filas de ambos DataFrames solo donde el 'cliente_id'
# existe en *ambos* DataFrames. Esto asegura un conjunto de datos consolidado y limpio
# donde cada venta tiene una información de cliente correspondiente y viceversa.
try:
    merged_df = pd.merge(ventas_df, clientes_df, on='cliente_id', how='inner')
    print("\nDataFrames combinados exitosamente usando 'cliente_id'.")
    print(f"Forma del DataFrame combinado: {merged_df.shape} (filas, columnas)")
except Exception as e:
    print(f"Error durante la operación de combinación: {e}")
    exit()

# --- 3. Identificar valores nulos/faltantes y formatos inconsistentes/atípicos ---
print("\n--- Verificando Valores Nulos Antes de la Limpieza ---")
# El método .isnull().sum() es excelente para obtener un recuento de valores faltantes por columna.
print(merged_df.isnull().sum())

# Nos enfocaremos en problemas comunes de calidad de datos:
# - 'fecha_venta': Debe ser una fecha válida y dentro de un rango histórico razonable.
# - 'importe': Idealmente, debe ser un valor numérico positivo.
# - Otras columnas pueden tener valores faltantes generales.

# --- 4. Corregir nulos mediante imputación promedio o descarte justificado ---

# Manejo de la columna 'importe':
# Verificaremos importes negativos y nulos.
if 'importe' in merged_df.columns:
    print(f"\nValores faltantes en 'importe' antes de la imputación: {merged_df['importe'].isnull().sum()}")
    
    # Primero, abordamos los importes negativos. Una venta típicamente no puede tener un valor negativo.
    # Los reemplazaremos con 0, asumiendo que son errores de entrada de datos.
    initial_negative_importes = (merged_df['importe'] < 0).sum()
    if initial_negative_importes > 0:
        merged_df.loc[merged_df['importe'] < 0, 'importe'] = 0
        print(f"Se corrigieron {initial_negative_importes} valores negativos en 'importe' a 0.")
    
    # Ahora, imputamos cualquier valor 'importe' faltante restante con la media de la columna.
    # La imputación por la media es una estrategia común para datos numéricos, pero ten en cuenta sus limitaciones.
    if merged_df['importe'].isnull().any():
        mean_importe = merged_df['importe'].mean()
        merged_df['importe'].fillna(mean_importe, inplace=True)
        print(f"Valores faltantes en 'importe' imputados con la media: {mean_importe:.2f}")

# Manejo de la columna 'nombre_cliente':
# Para columnas categóricas o de texto, la imputación por la media no tiene sentido.
# Llenaremos los valores 'nombre_cliente' faltantes con 'Desconocido' como un marcador de posición.
if 'nombre_cliente' in merged_df.columns and merged_df['nombre_cliente'].isnull().any():
    print(f"\nValores faltantes en 'nombre_cliente' antes de rellenar: {merged_df['nombre_cliente'].isnull().sum()}")
    merged_df['nombre_cliente'].fillna('Desconocido', inplace=True)
    print("Valores faltantes en 'nombre_cliente' rellenados con 'Desconocido'.")

# --- 5. Normalizar formatos de fecha a YYYY-MM-DD y manejar fechas fuera de rango ---

if 'fecha_venta' in merged_df.columns:
    print("\n--- Normalizando el formato de 'fecha_venta' y verificando valores atípicos ---")
    
    # Convertir 'fecha_venta' a objetos datetime. 'errors='coerce' convertirá
    # cualquier valor que no pueda ser analizado como fecha en 'NaT' (Not a Time), que es el nulo de pandas para fechas.
    merged_df['fecha_venta'] = pd.to_datetime(merged_df['fecha_venta'], errors='coerce')
    
    # Manejar fechas que se convirtieron en NaT después de la conversión (estos son los formatos inconsistentes)
    if merged_df['fecha_venta'].isnull().any():
        invalid_dates_count = merged_df['fecha_venta'].isnull().sum()
        # Para esta práctica, eliminaremos las filas con fechas no analizables.
        # En un escenario real, podrías investigar o intentar un análisis más avanzado.
        merged_df.dropna(subset=['fecha_venta'], inplace=True)
        print(f"Se eliminaron {invalid_dates_count} filas debido a formatos de 'fecha_venta' inválidos.")

    # Verificar fechas fuera de rango (por ejemplo, fechas futuras o extremadamente antiguas).
    # Definimos un rango razonable: desde el 1 de enero de 2000 hasta la fecha actual.
    min_date = pd.to_datetime('2000-01-01')
    # Usamos 'today' para obtener la fecha actual del sistema.
    max_date = pd.to_datetime('today') 
    
    # Identificar filas donde la 'fecha_venta' está fuera de nuestro rango válido definido.
    out_of_range_dates_count = merged_df[(merged_df['fecha_venta'] < min_date) | (merged_df['fecha_venta'] > max_date)].shape[0]
    if out_of_range_dates_count > 0:
        # Filtraremos el DataFrame para mantener solo las fechas válidas.
        merged_df = merged_df[(merged_df['fecha_venta'] >= min_date) & (merged_df['fecha_venta'] <= max_date)]
        print(f"Se eliminaron {out_of_range_dates_count} filas con 'fecha_venta' fuera del rango definido "
              f"({min_date.strftime('%Y-%m-%d')} a {max_date.strftime('%Y-%m-%d')}).")

    # Finalmente, normalizar el formato de fecha a YYYY-MM-DD.
    # Usamos .dt.strftime('%Y-%m-%d') en los objetos datetime.
    merged_df['fecha_venta'] = merged_df['fecha_venta'].dt.strftime('%Y-%m-%d')
    print("Se normalizó 'fecha_venta' al formato YYYY-MM-DD.")

# Verificación final de nulos después de todos los pasos de limpieza para asegurar que no se omitió nada.
print("\n--- Valores Nulos Después de Toda la Limpieza ---")
print(merged_df.isnull().sum())

# Mostrar un resumen del DataFrame limpio.
print("\n--- Información del DataFrame Limpio y Combinado ---")
merged_df.info()
print("\nPrimeras 5 filas del DataFrame limpio:")
print(merged_df.head())

# --- 6. Exportar el dataset limpio como dataset_integrado.csv ---
output_filename = 'dataset_integrado.csv'
try:
    # Usa index=False para evitar que pandas escriba el índice del DataFrame como una columna en el CSV.
    merged_df.to_csv(output_filename, index=False)
    print(f"\nDataset limpio exportado exitosamente como '{output_filename}'.")
except Exception as e:
    print(f"Error al exportar el dataset limpio: {e}")