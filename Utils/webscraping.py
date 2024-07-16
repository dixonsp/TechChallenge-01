from selenium import webdriver # importa módulo webdriver para realizar webscraping
from selenium.webdriver.chrome.options import Options # importa módulo Options para definição de parametros para a execução do browser chrome
from fastapi import HTTPException # importa módulo para o tratamento de exeções  HTTP
import json # importa módulo para tratamento de dados JSON
from selenium.webdriver.common.by import By # importa módulo By que traz alguns parãmetros para navegação durante o scraping
import time # importa o módulo time para ajudar a trabalhar com a tempo
import datetime # importa o módulo datetime para ajudar a trabalhar com datas
import pandas as pd # importa módulo pandas para trabalhar com dados de maneira simples
from lxml import etree # importa lxlm para trabalhar com conteúdo HTML
from io import StringIO # importa a biblioteca io para trabalhar com o sistema de arquivos
import pyarrow
import os
import sys
root = os.path.abspath("./..")
sys.path.append(root)
from model.negocio import Negocio, eTipo

class WebScraping():

    def __init__(self, tipo:eTipo=None, anoInicio:int=None, anoTermino:int=None):
        try:
            
            with open("../config/parametros.json", "r") as file: # cria contexto (with) para abertura de arquivo com parametros para fechar logo após o bloco
                data = json.load(file) #carrega os dados do arquivo de parâmetros
            
            self.anoInicio = anoInicio if anoInicio is not None else data['ano_inicio_scraping']
            self.anoTermino = anoTermino if anoTermino is not None else datetime.date.today().year                
            self.tipo = tipo  # Corrigido para garantir que self.tipo é atribuído corretamente
                
        except Exception as e:
            HTTPException(status_code=500, detail=str(e))

    def __private_save_html_content(self, negocio:Negocio, content:str, ano:int):
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
                arquivo = f"../data/html/{negocio.tipo}_{ano}.html"
            elif negocio.tipo in ("Processamento", "Importacao", "Exportacao"):
                    if (negocio.subtipo!=None):
                        arquivo = f"../data/html/{negocio.tipo}_{negocio.subtipo}_{ano}.html"
                    else:
                        raise HTTPException(status_code=500, detail=f"subtipo_negocio nao informado para o tipo_negocio {negocio.tipo}")
            else:
                raise HTTPException(status_code=500, detail=f"tipo_negocio `{negocio.tipo}` invalido")
            
            with open(arquivo, "w", encoding='utf-8') as file:
                file.write(content)

            return arquivo
        
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    def __private_getContent(self, url:str):
        options = Options()
        # executa o navegador sem a UI
        options.add_argument("--headless")
        # disabilita o sandbox
        options.add_argument("--no-sandbox")
        # disabilita dev shm
        options.add_argument("--disable-dev-shm-usage")
        # iniciar instancia do chrome
        driver = webdriver.Chrome(options=options)
        
        driver.get(url=url)
        time.sleep(2)
        html_content = driver.page_source
        driver.close()
        return html_content

    def __private_Scraping(self, negocio:Negocio, anoInicio:int=None, anoTermino:int=None):
        anoInicio = anoInicio if anoInicio is not None else self.anoInicio
        anoTermino = anoTermino if anoTermino is not None else self.anoTermino

        if negocio.tipo in("Producao", "Comercializacao"):
            print(f"inicio:{anoInicio} - termino{anoTermino}")
            for ano in range(anoInicio, anoTermino):
                print(f"ano->{ano}")
                url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{negocio.codigo}"
                html_content =  self.__private_getContent(url=url)
                self.__private_save_html_content(negocio, html_content, ano=ano)
        elif negocio.tipo in ("Processamento", "Importacao", "Exportacao"):
            for codigo_subtipo, nome_subtipo in negocio.subtipo:
                for ano in range(anoInicio, anoTermino):
                    url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{negocio.codigo}&subopcao=subopt_0{codigo_subtipo}"
                    html_content = self.__private_getContent(url=url)
                    self.__private_save_html_content(negocio=negocio, content=html_content, ano=ano)
        else:
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{negocio.tipo}` invalido")
        
    def __private_CreateParquetFile(self, nome_tipo:str, ano:int, table_elements:str, nome_subtipo:str=None):
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
            arquivo_parquet = f"../data/parquet/{nome_tipo}_{ano}.parquet"
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
            arquivo_parquet = f"../data/parquet/{nome_tipo}_{nome_subtipo}_{ano}.parquet"
        else:
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{nome_subtipo}` invalido")
        
        combined_df.to_parquet(arquivo_parquet, engine='pyarrow')

    def __private_ProcessHtmlFileToParquet(self, nome_tipo:str, ano:int, nome_subtipo:str=None):
        if nome_tipo in("Producao", "Comercializacao"):
            arquivo_html = f"../data/html/{nome_tipo}_{ano}.html"
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
            arquivo_html = f"../data/html/{nome_tipo}_{nome_subtipo}_{ano}.html"

        with open(arquivo_html, "r", encoding="utf-8") as htm_file:
            html_content = htm_file.read()
            parser = etree.HTMLParser()
            tree = etree.fromstring(html_content, parser)
            table_elements = tree.xpath('//table[contains(@class, "tb_base") and contains(@class, "tb_dados")]')
            num_tables = len(table_elements)
            self.__private_CreateParquetFile(nome_tipo=nome_tipo, ano=ano, table_elements=table_elements, nome_subtipo=nome_subtipo)
            
    def WebScaping(self):
        anoInicio = self.anoInicio
        anoTermino = self.anoTermino
        if self.tipo == eTipo.PRODUCAO or self.tipo == None:
            with Negocio(eTipo.PRODUCAO) as negocio:
                self.__private_Scraping(negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)
        
        if self.tipo == eTipo.PROCESSAMENTO or self.tipo == None:
            with Negocio(eTipo.PROCESSAMENTO) as negocio:
                self.__private_Scraping(negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

        if self.tipo == eTipo.COMERCIALIZACAO or self.tipo == None:
            with Negocio(eTipo.COMERCIALIZACAO) as negocio:
                self.__private_Scraping(negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

        if self.tipo == eTipo.IMPORTACAO or self.tipo == None:
            with Negocio(eTipo.IMPORTACAO) as negocio:
                self.__private_Scraping(negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

        if self.tipo == eTipo.EXPORTACAO or self.tipo == None:
            with Negocio(eTipo.EXPORTACAO) as negocio:
                self.__private_Scraping(negocio=negocio, anoInicio=anoInicio, anoTermino=anoTermino)

        if self.tipo == eTipo.PRODUCAO or self.tipo == None:
            with Negocio(eTipo.PRODUCAO) as negocio:
                for ano in range(anoInicio, anoTermino):
                    self.__private_ProcessHtmlFileToParquet(negocio=negocio, ano=ano)

        if self.tipo == eTipo.PROCESSAMENTO or self.tipo == None:
            with Negocio(eTipo.PROCESSAMENTO) as negocio:
                for subtipo in negocio.Subtipo:
                    for ano in range(anoInicio, anoTermino):
                        self.__private_ProcessHtmlFileToParquet(negocio=negocio, ano=ano, subtipo=subtipo['Nome'])

        if self.tipo == eTipo.COMERCIALIZACAO or self.tipo == None:
            with Negocio(eTipo.COMERCIALIZACAO) as negocio:
                for ano in range(anoInicio, anoTermino):
                    self.__private_ProcessHtmlFileToParquet(negocio=negocio, ano=ano)

        if self.tipo == eTipo.IMPORTACAO or self.tipo == None:
            with Negocio(eTipo.IMPORTACAO) as negocio:
                for subtipo in negocio.Subtipo:
                    for ano in range(anoInicio, anoTermino):
                        self.__private_ProcessHtmlFileToParquet(negocio=negocio, ano=ano, subtipo=subtipo['Nome'])

        if self.tipo == eTipo.EXPORTACAO or self.tipo == None:
            with Negocio(eTipo.EXPORTACAO) as negocio:
                for subtipo in negocio.Subtipo:
                    for ano in range(anoInicio, anoTermino):
                        self.__private_ProcessHtmlFileToParquet(negocio=negocio, ano=ano, subtipo=subtipo['Nome'])
