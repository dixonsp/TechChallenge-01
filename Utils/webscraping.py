from selenium import webdriver #importa módulo webdriver para realizar webscraping
from selenium.webdriver.chrome.options import Options #importa módulo Options para definição de parametros para a execução do browser chrome
from fastapi import HTTPException #importa módulo para o tratamento de exeções  HTTP
import json #importa módulo para tratamento de dados JSON
from selenium.webdriver.common.by import By #importa módulo By que traz alguns parãmetros para navegação durante o scraping
import time #importa o módulo time para ajudar a trabalhar com a tempo
import datetime #importa o módulo datetime para ajudar a trabalhar com datas
import pandas as pd #importa módulo pandas para trabalhar com dados de maneira simples
from lxml import etree #importa lxlm para trabalhar com conteúdo HTML
from io import StringIO #importa a biblioteca io para trabalhar com o sistema de arquivos
import pyarrow
import os
import sys
root = os.path.abspath("./..")
sys.path.append(root)
from model.negocio import Negocio, eTipo

with open("config/parametros.json", "r") as file: #cria contexto (with) para abertura de arquivo com parametros para fechar logo após o bloco
    data = json.load(file) #carrega os dados do arquivo de parâmetros

anoInicio = data['ano_inicio_scraping'] #recupera a data de início para webscraping
anoTermino = datetime.date.today().year #configura a data de término para webscraping= Ano Atual (no laço FOR o iterador não executa o último número, ou seja, se o ano termino for 2024 o iterador só processa até o ano de 2023. O FOR é inclusivo no limite inferior e exclusivo no limite superior->nao incluindo o último número do RANGE)

def save_html_content(negocio:Negocio, content:str, ano:int):
    """
    Função para salvar conteúdo HTML para posterior tratamento

    Args:
        negocio (str): o nome do negocio: (producao, processamento, comercializacao, importacao, exportacao)
        content (str): conteúdo HTML
        ano (int): ano da safra
        tipo_negocio (str - opcional com default=None): especialização de negocio
    
        

    Returns:
        str: caminho do arquivo gerado.

    Raises:
        HTTPException: Se ocorrer qualquer erro durante o processo de gravação.
    """

    try: #inicia tratamento de erro
        if negocio.tipo in("Producao", "Comercializacao"):
            arquivo = f"arquivos/{negocio.tipo}_{ano}.html"
        elif negocio.tipo in ("Processamento", "Importacao", "Exportacao"):
                if (negocio.subtipo!=None):
                    arquivo = f"arquivos/{negocio.tipo}_{negocio.subtipo}_{ano}.html"
                else:
                    raise HTTPException(status_code=500, detail=f"subtipo_negocio nao informado para o tipo_negocio {negocio.tipo}")
        else:
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{negocio.tipo}` invalido")
        
        with open(arquivo, "w", encoding='utf-8') as file:
            file.write(content)

        return arquivo
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def read_html_content(negocio:Negocio, ano:int = None):
    try: #inicia tratamento de erro
        if negocio.tipo in("Producao", "Comercializacao"):
            arquivo = f"arquivos/{negocio.tipo}_{ano}.html"
        elif negocio.tipo in ("Processamento", "Importacao", "Exportacao"):
            if (negocio.subtipo!=None):
                arquivo = f"arquivos/{negocio.tipo}_{negocio.subtipo}_{ano}.html"
            else:
                raise HTTPException(status_code=500, detail=f"subtipo_negocio nao informado para o tipo_negocio {negocio.tipo}")
        else:
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{negocio.tipo}` invalido")
        
        with open(arquivo, "r", encoding='utf-8') as file:
            return file
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def getContent(driver:webdriver, url:str):
    driver.get(url=url)
    time.sleep(2)
    return driver.page_source

def Scrap(driver:webdriver, negocio:Negocio, anoInicio:int = None, anoTermino:int = None):
    if negocio.tipo in("Producao", "Comercializacao"):
        print(f"inicio:{anoInicio} - termino{anoTermino}")
        for ano in range(anoInicio, anoTermino):
            print(f"ano->{ano}")
            url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{negocio.codigo}"
            html_content = getContent(driver=driver, url=url)
            save_html_content(negocio, html_content, ano=ano)
    elif negocio.tipo in ("Processamento", "Importacao", "Exportacao"):
        for codigo_subtipo, nome_subtipo in negocio.subtipo:
            for ano in range(anoInicio, anoTermino):
                url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{negocio.codigo}&subopcao=subopt_0{codigo_subtipo}"
                html_content = getContent(driver=driver, url=url)
                save_html_content(negocio=negocio, content=html_content, ano=ano)
    else:
        raise HTTPException(status_code=500, detail=f"tipo_negocio `{negocio.tipo}` invalido")
    
def CreateParquetFile(nome_tipo:str, ano:int, table_elements:str, nome_subtipo:str=None):
    # Lista para armazenar os DataFrames
    dfs = []

    for i, table in enumerate(table_elements):
        # Obter o HTML da tabela
        table_html = etree.tostring(table, pretty_print=True, encoding='unicode')
        
        # Converter o HTML da tabela em DataFrame usando pandas
        df = pd.read_html(StringIO(table_html))[0]
        dfs.append(df)

    # Opcional: combinar todos os DataFrames em um único DataFrame
    combined_df = pd.concat(dfs, ignore_index=True)

    if nome_tipo in("Producao", "Comercializacao"):
        arquivo_parquet = f"data/{nome_tipo}_{ano}.parquet"
    elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
        arquivo_parquet = f"data/{nome_tipo}_{nome_subtipo}_{ano}.parquet"
    else:
        raise HTTPException(status_code=500, detail=f"tipo_negocio `{nome_subtipo}` invalido")
    
    combined_df.to_parquet(arquivo_parquet, engine='pyarrow')

def ProcessHtmlFileToParquet(nome_tipo:str, ano:int, nome_subtipo:str=None):
    if nome_tipo in("Producao", "Comercializacao"):
        arquivo_html = f"arquivos/{nome_tipo}_{ano}.html"
    elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
        arquivo_html = f"arquivos/{nome_tipo}_{nome_subtipo}_{ano}.html"

    with open(arquivo_html, "r", encoding="utf-8") as htm_file:
        html_content = htm_file.read()
        parser = etree.HTMLParser()
        tree = etree.fromstring(html_content, parser)
        table_elements = tree.xpath('//table[contains(@class, "tb_base") and contains(@class, "tb_dados")]')
        num_tables = len(table_elements)
        CreateParquetFile(nome_tipo=nome_tipo, ano=ano, table_elements=table_elements, nome_subtipo=nome_subtipo)
        


def WebScaping():
    options = Options()
    #executa o navegador sem a UI
    options.add_argument("--headless")
    #disabilita o sandbox
    options.add_argument("--no-sandbox")
    #disabilita dev shm
    options.add_argument("--disable-dev-shm-usage")


    #iniciar o chrome
    driver = webdriver.Chrome(options=options)

    with Negocio(eTipo.PRODUCAO) as negocio:
        Scrap(driver=driver, negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)
    
    with Negocio(eTipo.PROCESSAMENTO) as negocio:
        Scrap(driver=driver, negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

    with Negocio(eTipo.COMERCIALIZACAO) as negocio:
        Scrap(driver=driver, negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

    with Negocio(eTipo.IMPORTACAO) as negocio:
        Scrap(driver=driver, negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

    with Negocio(eTipo.EXPORTACAO) as negocio:
        Scrap(driver=driver, negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

    driver.quit()

    with Negocio(eTipo.PRODUCAO) as negocio:
        for ano in range(anoInicio, anoTermino):
            ProcessHtmlFileToParquet(negocio=negocio, ano=ano)

    with Negocio(eTipo.PROCESSAMENTO) as negocio:
        for subtipo in negocio.Subtipo:
            for ano in range(anoInicio, anoTermino):
                ProcessHtmlFileToParquet(negocio=negocio, ano=ano, subtipo=subtipo['Nome'])

    with Negocio(eTipo.COMERCIALIZACAO) as negocio:
        for ano in range(anoInicio, anoTermino):
            ProcessHtmlFileToParquet(negocio=negocio, ano=ano)

    with Negocio(eTipo.IMPORTACAO) as negocio:
        for subtipo in negocio.Subtipo:
            for ano in range(anoInicio, anoTermino):
                ProcessHtmlFileToParquet(negocio=negocio, ano=ano, subtipo=subtipo['Nome'])

    with Negocio(eTipo.EXPORTACAO) as negocio:
        for subtipo in negocio.Subtipo:
            for ano in range(anoInicio, anoTermino):
                ProcessHtmlFileToParquet(negocio=negocio, ano=ano, subtipo=subtipo['Nome'])

    