""" obtener_fuentes.py: Obtener y guardar los archivos fuentes """
from logger import *                  # logging
from datetime import date             # fechas
import locale                         # meses en español
from pathlib import Path              # convertir str a Path
import requests                       # HTTP


def crear_path_y_filename(categoria):
    """
    Organizar los archivos en rutas siguiendo la siguiente estructura:
    'categoría\año-mes\categoria-dia-mes-año.csv'
    Por ejemplo: 'museos\2021-noviembre\museos-03-11-2021'
    Si el archivo existe debe reemplazarse. La fecha de la nomenclatura es la fecha de descarga.
    """

    hoy = date.today()
    anio = hoy.strftime('%Y')
    mes_numero = hoy.strftime('%m')
    dia = hoy.strftime('%d')
    # meses = ("enero", "febrero", "marzo", "abril", "mayo", "junio",
    #          "julio", "agosto", "setiembre", "octubre", "noviembre", "diciembre")
    # mes_letras = meses[hoy.month - 1]
    locale.setlocale(locale.LC_TIME, '')  # para que los meses salgan en español
    mes_letras = hoy.strftime('%B')
    path = categoria + '/' + anio + '-' + mes_letras + '/'
    logging.info(f"path={path}")
    try:
        if Path(path).exists():
            logging.info(f"path={path} -> existe")
        else:
            logging.info(f"creando -> path={path}")
            Path(path).mkdir(parents=True)
    except Exception as error:
        logging.exception(f"error:{error}")

    name = categoria + '-' + dia + '-' + mes_numero + '-' + anio + '.csv'
    logging.info(f"name={name}")
    filename = path + name
    logging.info(f"filename={filename}")
    return filename


def guardar_fuente(url, fullfilename):
    """
    Obtener los 3 archivos de fuente utilizando la librería requests y almacenarse en forma local
    (Ten en cuenta que las urls pueden cambiar en un futuro):
    Datos Argentina - Museos
    Datos Argentina - Salas de Cine
    Datos Argentina - Bibliotecas Populares
    """
    try:
        respuesta = requests.get(url)
        respuesta.encoding = 'utf-8'
        open(fullfilename, "wb").write(respuesta.content)
        logging.info(f"Guardando {fullfilename}")
    except Exception as error:
        logging.exception(f"error:{error}")

    return
