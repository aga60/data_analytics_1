import pandas as pd

from logger import *                  # logging
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy import create_engine, exc
from datetime import date
import psycopg2

def crear_db(conn: str) -> None:
    """
    Crea la base de datos si no existe
    :param conn: string de conexion para el motor SQLAlchemy de la forma 'postgresql://user:password@host/dbname'
    :return: nada
    """
    db_name = conn.split('/')[-1]
    try:
        if database_exists(conn):
            logging.info(f"Base de datos {db_name} -> existe")
        else:
            logging.info(f"Creando base de datos -> {db_name}")
            create_database(conn)

    except Exception as error:
        logging.exception(f"error:{error}")

    return


def dataframe_a_tabla(conn: str, df: pd.DataFrame, tabla: str) -> None:
    """
    Crea una tabla con el contenido del DataFrame
    :param conn: string de conexion para el motor SQLAlchemy de la forma 'postgresql://user:password@host/dbname'
    :param df: DataFrame resultado
    :param tabla: tabla en la base de datos
    :return: nada
    """
    engine = create_engine(conn)
    try:
        logging.info(f"Creando tabla -> {tabla}")
        df.to_sql(tabla, con=engine, if_exists='replace')
    except exc.SQLAlchemyError as error:
        logging.exception(f"error:{error}")

    return


def ejecutar_sql_script(conn: str, script: str) -> None:
    """
    Ejecuta un script SQL para crear las tablas en la base de datos
    :param conn: string de conexion para el motor SQLAlchemy de la forma 'postgresql://user:password@host/dbname'
    :param script: script SQL para la creacion de las tablas
    :return: nada
    """
    try:
        logging.info(f"Ejecutando script -> {script}")
        engine = create_engine(conn)
        # check for /* */
        with open(script, 'r') as f:
            assert '/*' not in f.read(), 'comments with /* */ not supported in SQL file python interface'

        # we check out the SQL file line-by-line into a list of strings (without \n, ...)
        with open(script, 'r') as f:
            queries = [line.strip() for line in f.readlines()]

        # from each line, remove all text which is behind a '--'
        def cut_comment(query: str) -> str:
            idx = query.find('--')
            if idx >= 0:
                query = query[:idx]
            return query

        # join all in a single line code with blank spaces
        queries = [cut_comment(q) for q in queries]
        sql_command = ' '.join(queries)

        # execute in connection (e.g. sqlalchemy)
        engine.execute(sql_command)


    except exc.SQLAlchemyError as error:
        logging.exception(f"error:{error}")

    return


def actualizar_tabla(conn: str, df: pd.DataFrame, tabla: str) -> None:
    """
    Actualiza la tabla con los datos del DataFrame
    Todos los registros existentes deben ser reemplazados por la nueva informaci??n.
    Dentro de cada tabla debe indicarse en una columna adicional la fecha de carga.
    Los registros para los cuales la fuente no brinda informaci??n deben cargarse como nulos.
    :param conn: string de conexion para el motor SQLAlchemy de la forma 'postgresql://user:password@host/dbname'
    :param df: DataFrame a cargar en la tabla
    :param tabla: en la que se carga el DataFrame
    :return: nada
    """
    df['fecha_de_carga'] = date.today().strftime("%d-%m-%Y")

    try:
        logging.info(f"Actualizando tabla -> {tabla}")
        df.to_sql(tabla, conn, if_exists='replace')

    except exc.SQLAlchemyError as error:
        logging.error(f"load.py -> load(): Error: {error}")

