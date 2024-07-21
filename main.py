import os
import sys
from model.negocio import eTipo
import logging
from datetime import datetime

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

agora = datetime.now()
data_hora_formatada = agora.strftime("%Y-%m-%d %H:%M:%S")


root = os.path.abspath("./Utils")
sys.path.append(root)

from fastapi import FastAPI, Query
from fastapi import HTTPException
from Utils.webscraping import WebScraping


app = FastAPI()

@app.get("/")
def root():
    return {'TechChallenge 01':'WEB Scraping, Spark, RDBMS, API, JWT, AWS, Data Lake'}

@app.get("/scraping")
def scraping(tipo:eTipo= Query(...), anoInicio:int=2022, anoTermino:int=2022):
    logger.debug("LOG:INICIANDO WebScraping")

    print(f'INICIANDO WebScraping: {data_hora_formatada}')

    scraping = WebScraping(anoInicio=anoInicio, anoTermino=anoTermino, tipo=None)

    if anoInicio==2022:
        tipoSelecionado = tipo
        print(f"---TIPO CERTO->{tipo.value}")

    scraping.WebScaping(tipo=tipo ,anoInicio=anoInicio, anoTermino=anoTermino)
    print(f'WebScraping FINALIZADO: {data_hora_formatada}')
    logger.debug("LOG:WebScraping FINALIZADO")
    