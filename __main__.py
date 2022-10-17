from logger import *                  # logging
from decouple import config           # para leer constantes en .env
import obtener_fuentes as ob
import procesar_datos as pr
import pandas as pd
import usar_basedatos as us


logging.warning('')
logging.warning('Ejecución -> iniciada')

# CONSTANTES
CATEGORIAS = config('CATEGORIAS', cast=lambda v: [s.strip() for s in v.split(',')])
logging.info(f"CATEGORIAS: {CATEGORIAS}")

URL_MUSEOS = config('URL_MUSEOS')
URL_SALAS_DE_CINE = config('URL_SALAS_DE_CINE')
URL_BIBLIOTECAS_POPULARES = config('URL_BIBLIOTECAS_POPULARES')
URLS = [URL_MUSEOS, URL_SALAS_DE_CINE, URL_BIBLIOTECAS_POPULARES]
logging.info(f"URLS: {URLS}")

PATH_FUENTES_DATOS = config('PATH_FUENTES_DATOS')
logging.info(f"PATH_FUENTES_DATOS: {PATH_FUENTES_DATOS}")

DB_USER = config('DB_USER')
DB_PASS = config('DB_PASS')
DB_HOST = config('DB_HOST')
DB_PORT = config('DB_PORT')
DB_NAME = config('DB_NAME')
DB_CONN = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:" \
                f"{DB_PORT}/{DB_NAME}"


# obtener = True: en produccion, obtener siempre los archivos fuente
# obtener = False: en desarrollo, ya tengo los archivos fuente, los cargo desde el almacenamiento
obtener = True


def main():
    # Crear un DataFrame unificado
    conjunto_df = {}
    for i in range(len(CATEGORIAS)):
        # nombres para los archivos fuentes de datos
        filename = ob.crear_path_y_filename(CATEGORIAS[i])
        if obtener:
            ob.guardar_fuente(URLS[i], PATH_FUENTES_DATOS + filename)
        else:
            pass

        # Conjunto con los DataFrame de cada categoria
        conjunto_df[i] = pd.DataFrame(pr.normalizar_datos(CATEGORIAS[i], PATH_FUENTES_DATOS + filename))

    df_unificado = pd.concat(conjunto_df)

    df_registros_por_categoria = pr.registros_por_categoria(df_unificado)
    df_registros_por_fuente = pr.registros_por_fuente(df_unificado)
    df_registros_por_provincia_y_categoria = pr.registros_por_provincia_y_categoria(df_unificado)

    # Crear un dataframe con datos de cines
    filename_cines = ob.crear_path_y_filename('salas_de_cine')
    df_cines = pr.procesar_datos_cines(PATH_FUENTES_DATOS + filename_cines)

    us.crear_db(DB_CONN)

    # Con los dataframes crear las tablas en la base de datos
    # -- usando to_sql se crea la tabla y se cargan los datos --

    us.dataframe_a_tabla(DB_CONN, df_unificado, 'unificada')
    us.dataframe_a_tabla(DB_CONN, df_registros_por_categoria, 'registros_por_categoria')
    us.dataframe_a_tabla(DB_CONN, df_registros_por_fuente, 'registros_por_fuente')
    us.dataframe_a_tabla(DB_CONN, df_registros_por_provincia_y_categoria, 'registros_por_provincia_y_categoria')
    us.dataframe_a_tabla(DB_CONN, df_cines, 'cines')

    # Crear tablas mediante script .sql
    script = 'crear_tablas.sql'
    us.ejecutar_sql_script(DB_CONN, script)

    # Actualizar los datos en las tablas
    us.actualizar_tabla(DB_CONN, df_unificado, 'unificada')
    us.actualizar_tabla(DB_CONN, df_registros_por_categoria, 'registros_por_categoria')
    us.actualizar_tabla(DB_CONN, df_registros_por_fuente, 'registros_por_fuente')
    us.actualizar_tabla(DB_CONN, df_registros_por_provincia_y_categoria, 'registros_por_provincia_y_categoria')
    us.actualizar_tabla(DB_CONN, df_cines, 'cines')

    logging.warning('Ejecución -> finalizada')
    logging.shutdown()


if __name__ == '__main__':
    main()
