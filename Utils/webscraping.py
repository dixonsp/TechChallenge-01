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
root = os.path.abspath("./..")
sys.path.append(root)
from model.negocio import Negocio, eTipo

class WebScraping():

    def __init__(self, tipo:eTipo=None, anoInicio:int=None, anoTermino:int=None, persistHtml:bool=None, persistParquet:bool=None):
        #try:
        with open("config/parametros.json", "r") as file: # cria contexto (with) para abertura de arquivo com parametros para fechar logo após o bloco
            data = json.load(file) #carrega os dados do arquivo de parâmetros
        
        self.anoInicio = anoInicio if anoInicio is not None else int(data['ano_inicio_scraping'])
        self.anoTermino = anoTermino if anoTermino is not None else datetime.now().year                
        self.tipo = tipo  # Corrigido para garantir que self.tipo é atribuído corretamente
        self.persistHtml = persistHtml
        self.persistParquet = persistParquet
                
        #except Exception as e:
        #    HTTPException(status_code=500, detail=str(e))

    def __private_save_html_content(self, nome_tipo:str, content:str, ano:int, nome_subtipo:str=None) -> str:
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

    def __private_CreateParquetFile(self, nome_tipo:str, ano:int, df:any, nome_subtipo:str=None):
        if nome_tipo in("Producao", "Comercializacao"):
            arquivo_parquet = f"data/parquet/{nome_tipo}_{ano}.parquet"
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
            arquivo_parquet = f"data/parquet/{nome_tipo}_{nome_subtipo}_{ano}.parquet"
        else:
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{nome_subtipo}` invalido")
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
            self.__private_save_html_content(nome_tipo=nome_tipo, content=html_content, ano=ano, nome_subtipo=nome_subtipo)
        
        df = self.__private_ProcessHtml(nome_tipo=nome_tipo, ano=ano, html_content=html_content)
        #if persistParquet:
        #    self.__private_CreateParquetFile(nome_tipo=nome_tipo, ano=ano, nome_subtipo=nome_subtipo)
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

            
    def WebScaping(self, tipo:eTipo=None, anoInicio:int=None, anoTermino:int=None, persistHtml:bool=False, persistParquet:bool=False):
        
        anoInicio = self.anoInicio
        anoTermino = self.anoTermino
        tipo = self.tipo
        persistHtml = self.persistHtml
        persistParquet = self.persistParquet

        print(f"*** INICIANDO WEBSCRAPING->{tipo.value} -> Ano Inicio -> {anoInicio} Ano Termino {anoTermino}")

        agora = datetime.now()

        if anoTermino!=None:
            if anoInicio > anoTermino:
                raise HTTPException(status_code=500, detail=f"Ano Início nao pode ser maior do que Ano Término.")
            if anoTermino > agora.year:
                raise HTTPException(status_code=500, detail=f"Ano Término não pode ser maior do que o Ano Atual.")

        df = None

        if tipo == eTipo.PRODUCAO or tipo == eTipo.COMERCIALIZACAO:
            dfIteracoes = []
            for ano in range(anoInicio, anoTermino+1):
                print(f"****** Scraping: {tipo} ano: {ano} até: {anoTermino}")
                with Negocio(tipo) as negocio:
                    df = self.__private_Scraping(codigo_tipo=negocio.codigo, nome_tipo=negocio.tipo, ano=ano, codigo_subtipo=None, nome_subtipo=None, persistHtml=persistHtml, persistParquet=persistParquet)
                    if df is not None and len(df)>0: 
                        dfIteracoes.append(df)
            arquivo_parquet = f"data/parquet/{negocio.tipo}_{anoInicio}{f'_{anoTermino}' if anoTermino is not None and anoTermino != anoInicio else ''}.parquet"
        
        elif tipo == eTipo.PROCESSAMENTO or tipo == eTipo.IMPORTACAO or tipo == eTipo.EXPORTACAO:
            dfIteracoes = []
            with Negocio(tipo) as negocio:
                for subtipo in negocio.subtipo:
                    for ano in range(anoInicio, anoTermino+1):
                        print(f"****** Scraping:{tipo}, {subtipo} ano: {ano} até: {anoTermino}")
                        df = self.__private_Scraping(codigo_tipo=negocio.codigo, nome_tipo=negocio.tipo, ano=ano, codigo_subtipo=subtipo["codigo"], nome_subtipo=subtipo["nome"], persistHtml=persistHtml, persistParquet=persistParquet)
                        if df is not None and len(df)>0: 
                            dfIteracoes.append(df)
                arquivo_parquet = f"data/parquet/{negocio.tipo}_{anoInicio}{f'_{anoTermino}' if anoTermino is not None and anoTermino != anoInicio else ''}.parquet"

        df = pd.concat(dfIteracoes, ignore_index=True)
        if persistParquet:
            df.to_parquet(arquivo_parquet, engine='pyarrow')