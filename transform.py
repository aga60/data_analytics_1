""" transform.py: Realiza el procesamiento de los datos"""
from logger import *                  # logging
import pandas as pd
# import numpy as np


def normalizar_datos(categoria: str, fullfilename: str) -> pd.DataFrame:
    """
    Crea dataframe a partir del archivo fuente guardado
    Elimina columnas no deseadas
    Renombra columnas

    Normalizar toda la información de Museos, Salas de Cine y Bibliotecas Populares,
    para crear una única tabla que contenga:
    cod_localidad
    id_provincia
    id_departamento
    categoría
    provincia
    localidad
    nombre
    domicilio
    código postal
    número de teléfono
    mail
    web
    * tengo que mantener 'fuente'
    :param categoria: categoria del archivo de datos
    :param fullfilename: path y filename de donde recuperar el archivo
    :return: pd.DataFrame: DataFrame producto de la normalizacion
    """

    df = pd.read_csv(fullfilename, encoding='UTF-8')
    logging.info(f"Dataframe {categoria} -> creado")
    # logging.info(f"df.shape antes={df.shape}")
    # logging.info(f"df.columns antes={df.columns}")

    if categoria == 'museos':
        df.drop(['piso',
                 'cod_area',
                 'latitud',
                 'longitud',
                 'tipo_latitud_longitud',
                 'tipo_gestion',
                 'departamento',
                 'año_actualizacion'],
                axis=1, inplace=True)

        df.rename(columns={'cod_loc': 'cod_localidad',
                           'categoria': 'categoría',
                           'cp': 'código postal',
                           'telefono': 'número de teléfono'},
                  inplace=True)

    elif categoria == 'salas_de_cine':
        df.drop(['departamento',
                 'piso',
                 'latitud',
                 'longitud',
                 'tipo_latitud_longitud',
                 'sector',
                 'tipo_de_gestion',
                 'año_actualizacion',
                 'butacas',
                 'pantallas',
                 'espacio_incaa'],
                axis=1, inplace=True)

        df.rename(columns={'categoria': 'categoría',
                           'direccion': 'domicilio',
                           'cp': 'código postal'},
                  inplace=True)

    elif categoria == 'bibliotecas_populares':
        df.drop(['Observacion',
                 'Subcategoria',
                 'Departamento',
                 'Piso',
                 'Cod_tel',
                 'Información adicional',
                 'Latitud',
                 'Longitud',
                 'TipoLatitudLongitud',
                 'Tipo_gestion',
                 'año_inicio',
                 'Año_actualizacion'],
                axis=1, inplace=True)

        df.rename(columns={'Cod_Loc': 'cod_localidad',
                           'IdProvincia': 'id_provincia',
                           'IdDepartamento': 'id_departamento',
                           'Categoría': 'categoría',
                           'Provincia': 'provincia',
                           'Localidad': 'localidad',
                           'Nombre': 'nombre',
                           'Domicilio': 'domicilio',
                           'CP': 'código postal',
                           'Teléfono': 'número de teléfono',
                           'Mail': 'mail',
                           'Web': 'web',
                           'Fuente': 'fuente'},
                  inplace=True)
    logging.info(f"Dataframe {categoria} -> normalizado")
    # logging.info(f"df.shape despues={df.shape}")
    # logging.info(f"df.columns despues={df.columns}")

    return df #df normalizado


def registros_por_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos conjuntos para poder generar una tabla con la siguiente información:
    Cantidad de registros totales por categoría
    :param df: dataframe normalizado
    :return: df: dataframe con registros por categoria
    """
    # Cantidad de registros totales por categoría
    df = df.groupby(by='categoría', as_index=False).size()
    df.rename(columns={'size': 'cantidad'}, inplace=True)
    logging.info(f"Dataframe: totales por categoría -> creado")
    return df


def registros_por_fuente(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos conjuntos para poder generar una tabla con la siguiente información:
    Cantidad de registros totales por fuente
    :param df: dataframe normalizado
    :return: df: dataframe con registros por fuente
    """
    # Cantidad de registros totales por fuente
    df = df.groupby(by='fuente', as_index=False).size()
    df.rename(columns={'size': 'cantidad'}, inplace=True)
    logging.info(f"Dataframe: totales por fuente -> creado")
    return df


def registros_por_provincia_y_categoria(df: pd.DataFrame) -> pd.DataFrame:
    """
    Procesa los datos conjuntos para poder generar una tabla con la siguiente información:
    Cantidad de registros por provincia y categoría
    :param df: dataframe normalizado
    :return: df: dataframe con registros por provincia y categoria
   """
    # Cantidad de registros totales por provincia y categoría
    df = df.groupby(by=['provincia', 'categoría'], as_index=False).size()
    df.rename(columns={'size': 'cantidad'}, inplace=True)
    logging.info(f"Dataframe: totales por provincia y categoria -> creado")
    return df


def procesar_datos_cines(fullfilename: str) -> pd.DataFrame:
    """
    Procesa la información de cines para poder crear una tabla que contenga:
    Provincia
    Cantidad de pantallas
    Cantidad de butacas
    Cantidad de espacios INCAA
    :param fullfilename: del archivo de datos de cines
    :return: df: dataframe con registros de cines por provincia
    """
    df = pd.read_csv(fullfilename, encoding='UTF-8', usecols=['provincia',
                                                               'pantallas',
                                                               'butacas',
                                                               'espacio_incaa'])
    logging.info(f"Dataframe: cines -> creado")

    df['espacio_incaa'].replace(to_replace='Si', value=True, inplace=True)
    df['espacio_incaa'].replace(to_replace='No', value=False, inplace=True)

    df.rename(columns={'provincia':'Provincia',
                       'pantallas':'Pantallas',
                       'butacas':'Butacas',
                       'espacio_incaa':'Espacios INCAA'},
              inplace=True)

    df = df.groupby(['Provincia'], as_index=False).sum()
    logging.info(f"Dataframe: cines: Provincia Pantallas Butacas Espacios INCAA -> creado")

    return df
