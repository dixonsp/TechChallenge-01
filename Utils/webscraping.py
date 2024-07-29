from selenium import webdriver # importa módulo webdriver para realizar webscraping
from selenium.webdriver.chrome.options import Options # importa módulo Options para definição de parametros para a execução do browser chrome
from fastapi import HTTPException # importa módulo para o tratamento de exeções  HTTP
import json # importa módulo para tratamento de dados JSON
from selenium.webdriver.common.by import By # importa módulo By que traz alguns parãmetros para navegação durante o scraping
import time # importa o módulo time para ajudar a trabalhar com a tempo
from datetime import datetime # importa o módulo datetime para ajudar a trabalhar com datas
import pandas as pd # importa módulo pandas para trabalhar com dados de maneira simples
from lxml import etree # importa lxlm para trabalhar com conteúdo HTML
from io import StringIO # importa a biblioteca io para trabalhar com o sistema de arquivos
import pyarrow
import os
import sys
#root = os.path.abspath("./..")
#sys.path.append(root)
from model.negocio import Negocio, eTipo
from Utils.utils import conectar_sqlite
from Utils.parquetprocessing import retorna_arquivos, processa_bronze

class WebScraping():

    def __init__(self, tipo:eTipo=None, persistHtml:bool=None, persistParquet:bool=None, lerHTML_SemScraping:bool=None, dbconnection:any=None):
        with open("config/parametros.json", "r") as file: # cria contexto (with) para abertura de arquivo com parametros para fechar logo após o bloco
            data = json.load(file) #carrega os dados do arquivo de parâmetros

        self.anoInicio = int(data['ano_inicio_scraping'])
        self.anoTermino = datetime.now().year                
        self.tipo = tipo
        self.persistHtml = persistHtml
        self.persistParquet = persistParquet
        self.lerHTML_SemScraping=lerHTML_SemScraping
        self.dbconnection = dbconnection
                
    def __private_save_html_content(self, content:str, nome_tipo:str, ano:int, nome_subtipo:str=None) -> str:
        """
        Função para salvar conteúdo HTML para posterior tratamento

        Args:
            nome_tipo (str): o nome do tipo: (producao, processamento, comercializacao, importacao, exportacao)
            content (str): conteúdo HTML
            ano (int): ano da safra
            nome_subtipo (str - opcional com default=None): especialização de tipo
        
        Returns:
            str: caminho e nome do arquivo gerado.

        Raises:
            HTTPException: Se ocorrer qualquer erro durante o processo de gravação.
        """

        #try: #inicia tratamento de erro
        if nome_tipo in("Producao", "Comercializacao"):
            arquivo = f"data/html/{nome_tipo}_{ano}.html"
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
                if (nome_subtipo!=None):
                    arquivo = f"data/html/{nome_tipo}_{nome_subtipo}_{ano}.html"
                else:
                    raise HTTPException(status_code=500, detail=f"subtipo nao informado para o tipo {nome_tipo}")
        else:
            raise HTTPException(status_code=500, detail=f"tipo `{nome_tipo}` invalido")
        
        with open(arquivo, "w", encoding='utf-8') as file:
            file.write(content)

        return arquivo
        
        #except Exception as e:
        #    raise HTTPException(status_code=500, detail=str(e))

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

    def __private_CreateParquetFile(self, tipo:eTipo, anoInicio:int, anoTermino:int, df:any, nome_subtipo:str=None):
        if tipo in(eTipo.PRODUCAO, eTipo.COMERCIALIZACAO):
            arquivo_parquet = f"data/parquet/bronze/{tipo.nome}_{anoInicio}{f'_{anoTermino}' if anoTermino is not None and anoTermino != anoInicio else ''}.parquet"
        elif tipo in (eTipo.PROCESSAMENTO, eTipo.IMPORTACAO, eTipo.EXPORTACAO):
            arquivo_parquet = f"data/parquet/bronze/{tipo.nome}_{nome_subtipo}_{anoInicio}{f'_{anoTermino}' if anoTermino is not None and anoTermino != anoInicio else ''}.parquet"
        df.to_parquet(arquivo_parquet, engine='pyarrow')

    def __private_Scraping(self, codigo_tipo:int, nome_tipo:str, ano:int, codigo_subtipo:int=None, nome_subtipo:str=None, persistHtml:bool=False, persistParquet:bool=False):
        if nome_tipo in("Producao", "Comercializacao"):
            url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{codigo_tipo}"
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
            url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{codigo_tipo}&subopcao=subopt_0{codigo_subtipo}"
        else:
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{nome_tipo}` invalido")
        
        html_content =  self.__private_getContent(url=url)
        if persistHtml:
            self.__private_save_html_content(content=html_content, nome_tipo=nome_tipo, ano=ano, nome_subtipo=nome_subtipo)
        
        df = self.__private_ProcessHtml(nome_tipo=nome_tipo, ano=ano, html_content=html_content)
        return df
        
    def __private_ProcessHtml(self, nome_tipo:str, ano:int, html_content:str, nome_subtipo:str=None, persistParquet:bool=None):
        parser = etree.HTMLParser()
        tree = etree.fromstring(html_content, parser)
        table_elements = tree.xpath('//table[contains(@class, "tb_base") and contains(@class, "tb_dados")]')

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
        return combined_df

            
    def WebScaping(self, tipo:eTipo=None, persistHtml:bool=False, persistParquet:bool=False, lerHTML_SemScraping:bool=True):
        
        anoInicio = self.anoInicio
        anoTermino = self.anoTermino
        tipo = self.tipo
        persistHtml = self.persistHtml
        persistParquet = self.persistParquet

        #arquivos_bronze = parquetprocessing.processa_bronze(tipo, 'data/parquet/bronze')

        #if arquivos_bronze != None:
            #print(f"*** ARQUIVO PARQUET NA CAMADA BRONZE ENCONTRADO->INICIANDO LEITURA->{tipo.nome}")
            #arquivos_silver = parquetprocessing.processa_silver(tipo, 'data/parquet/silver')
                #df = pd.read_parquet(arquivo_silver)
            
        arquivos = retorna_arquivos(tipo=tipo, path='data/parquet/silver')
        if len(arquivos) == 1:
            print(f"Arquivo(s) na camada silver encontrado(s). Só pode existir 1 arquivo de cada tipo no formato parquet na pasta.")
            df = pd.read_parquet(arquivos[0])
            return df.to_json()

        arquivos = retorna_arquivos(tipo=tipo, path='data/parquet/bronze')
        if len(arquivos) > 1:
            print(f"Arquivo(s) na camada bronze encontrado(s). Processando para gerar a camada Silver.")
            processa_bronze(tipo, 'data/parquet/bronze')
            return
        
        print(f"*** INICIANDO WEBSCRAPING->{tipo.nome} -> Ano Inicio -> {anoInicio} Ano Termino {anoTermino}")

        df = None

        if tipo in(eTipo.PRODUCAO, eTipo.COMERCIALIZACAO):
            dfIteracoes = []
            for ano in range(anoInicio, anoTermino+1):
                print(f"****** Scraping: {tipo.nome} ano: {ano} até: {anoTermino}")
                with Negocio(tipo) as negocio:
                    df = self.__private_Scraping(codigo_tipo=negocio.codigo, nome_tipo=negocio.tipo, ano=ano, codigo_subtipo=None, nome_subtipo=None, persistHtml=persistHtml, persistParquet=persistParquet)
                    if df is not None and len(df)>0: 
                        df["ano"] = ano
                        df["tipo"] = negocio.tipo
                        dfIteracoes.append(df)
            df = pd.concat(dfIteracoes, ignore_index=True)
            df = df.astype(str)
            if persistParquet:
                self.__private_CreateParquetFile(tipo=tipo, anoInicio=anoInicio, anoTermino=anoTermino, df=df, nome_subtipo=None)
                processa_bronze(tipo, 'data/parquet/bronze')

        elif tipo in(eTipo.PROCESSAMENTO, eTipo.IMPORTACAO, eTipo.EXPORTACAO):
            with Negocio(tipo) as negocio:
                for subtipo in negocio.subtipo.value:
                    dfIteracoes = []
                    for ano in range(anoInicio, anoTermino+1):
                        print(f"****** Scraping:{tipo.nome}, {subtipo["nome"]} ano: {ano} até: {anoTermino}")
                        df = self.__private_Scraping(codigo_tipo=negocio.codigo, nome_tipo=negocio.tipo, ano=ano, codigo_subtipo=subtipo["codigo"], nome_subtipo=subtipo["nome"], persistHtml=persistHtml, persistParquet=persistParquet)
                        if df is not None and len(df)>0: 
                            df["ano"] = ano
                            df["tipo"] = negocio.tipo
                            df["subtipo"] = subtipo["nome"]
                            dfIteracoes.append(df)
                    df = pd.concat(dfIteracoes, ignore_index=True)
                    df = df.astype(str)
                    if persistParquet:
                        self.__private_CreateParquetFile(tipo=tipo, anoInicio=anoInicio, anoTermino=anoTermino, df=df, nome_subtipo=subtipo["nome"])
                        processa_bronze(tipo, 'data/parquet/bronze')
        
        arquivos = retorna_arquivos(tipo=tipo, path='data/parquet/silver')
        if len(arquivos) == 1:
            print(f"Arquivo(s) na camada silver encontrado(s). Só pode existir 1 arquivo de cada tipo no formato parquet na pasta.")
            df = pd.read_parquet(arquivos[0])
            return df.to_json()