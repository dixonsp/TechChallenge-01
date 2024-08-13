# Importação das bibliotecas necessárias para o web scraping e manipulação de dados
import streamlit as st  # Biblioteca para criar interfaces de usuário interativas
from selenium import webdriver  # Módulo webdriver para realizar web scraping
from selenium.webdriver.chrome.options import Options  # Módulo para definir opções de execução do navegador Chrome
from fastapi import HTTPException  # Módulo para tratamento de exceções HTTP
import json  # Módulo para manipulação de dados JSON
from selenium.webdriver.common.by import By  # Módulo que fornece parâmetros de navegação durante o scraping
import time  # Módulo para manipulação de tempo (delays)
from datetime import datetime  # Módulo para manipulação de datas e horários
import pandas as pd  # Módulo para manipulação de dados em DataFrames
from lxml import etree  # Módulo para processamento de conteúdo HTML
from io import StringIO  # Módulo para manipulação de fluxos de entrada e saída de texto
import pyarrow  # Biblioteca para trabalhar com arquivos Parquet
import os  # Módulo para manipulação de sistema de arquivos
import sys  # Módulo para manipulação de operações relacionadas ao sistema
from model.negocio import Negocio, eTipo  # Importa classes personalizadas para manipulação de negócios e tipos de negócios
from OLD.utils import conectar_sqlite  # Importa função personalizada para conexão com SQLite
from Utils.parquetprocessing import retorna_arquivos, processa_bronze  # Importa funções para processamento de arquivos Parquet

# Classe para realizar operações de Web Scraping
class WebScraping():
    """
    Classe para realizar Web Scraping de dados relacionados a diferentes tipos de negócios,
    processando e salvando os dados em formatos HTML e Parquet.
    """

    def __init__(self, tipo:eTipo=None, persistHtml:bool=None, persistParquet:bool=None, lerHTML_SemScraping:bool=None, dbconnection:any=None):
        """
        Inicializa a classe WebScraping com os parâmetros fornecidos.

        :param tipo: Tipo de negócio para realizar o scraping.
        :param persistHtml: Define se o conteúdo HTML deve ser salvo.
        :param persistParquet: Define se os dados devem ser salvos em formato Parquet.
        :param lerHTML_SemScraping: Define se o HTML deve ser lido sem scraping.
        :param dbconnection: Conexão com o banco de dados, se necessário.
        """
        with open("config/parametros.json", "r") as file:  # Abre o arquivo de parâmetros de configuração
            data = json.load(file)  # Carrega os dados do arquivo JSON

        self.anoInicio = int(data['ano_inicio_scraping'])  # Define o ano inicial para o scraping
        self.anoTermino = datetime.now().year  # Define o ano final como o ano atual
        self.tipo = tipo  # Define o tipo de negócio
        self.persistHtml = persistHtml  # Define se deve persistir o conteúdo HTML
        self.persistParquet = persistParquet  # Define se deve persistir os dados em formato Parquet
        self.lerHTML_SemScraping = lerHTML_SemScraping  # Define se deve ler HTML sem realizar scraping
        self.dbconnection = dbconnection  # Conexão com o banco de dados

    # Função privada para salvar conteúdo HTML em arquivos
    def __private_save_html_content(self, content:str, nome_tipo:str, ano:int, nome_subtipo:str=None) -> str:
        """
        Salva o conteúdo HTML em um arquivo para posterior processamento.

        :param content: Conteúdo HTML a ser salvo.
        :param nome_tipo: Nome do tipo de negócio (ex.: Producao, Processamento).
        :param ano: Ano relacionado ao conteúdo.
        :param nome_subtipo: Nome do subtipo, se aplicável.
        :return: Caminho e nome do arquivo gerado.
        :raises HTTPException: Se ocorrer qualquer erro durante o processo de gravação.
        """
        if nome_tipo in ("Producao", "Comercializacao"):
            arquivo = f"data/html/{nome_tipo}_{ano}.html"  # Define o nome do arquivo para Producao e Comercializacao
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
            if nome_subtipo:
                arquivo = f"data/html/{nome_tipo}_{nome_subtipo}_{ano}.html"  # Define o nome do arquivo para Processamento, Importacao e Exportacao
            else:
                raise HTTPException(status_code=500, detail=f"Subtipo não informado para o tipo {nome_tipo}")  # Lança exceção se subtipo não for informado
        else:
            raise HTTPException(status_code=500, detail=f"Tipo `{nome_tipo}` inválido")  # Lança exceção se o tipo for inválido
        
        with open(arquivo, "w", encoding='utf-8') as file:  # Abre o arquivo para escrita
            file.write(content)  # Escreve o conteúdo HTML no arquivo

        return arquivo  # Retorna o caminho do arquivo gerado

    # Função privada para obter conteúdo HTML de uma URL
    def __private_getContent(self, url:str):
        """
        Obtém o conteúdo HTML de uma página web usando Selenium.

        :param url: URL da página web a ser acessada.
        :return: Conteúdo HTML da página.
        """
        options = Options()  # Inicializa as opções do navegador Chrome
        options.add_argument("--headless")  # Executa o navegador em modo headless (sem interface gráfica)
        options.add_argument("--no-sandbox")  # Desabilita o sandbox para segurança
        options.add_argument("--disable-dev-shm-usage")  # Desabilita o uso de /dev/shm
        driver = webdriver.Chrome(options=options)  # Inicia uma instância do navegador Chrome com as opções definidas
        driver.get(url=url)  # Acessa a URL fornecida
        time.sleep(2)  # Espera 2 segundos para garantir o carregamento da página
        html_content = driver.page_source  # Obtém o conteúdo HTML da página
        driver.close()  # Fecha o navegador
        return html_content  # Retorna o conteúdo HTML

    # Função privada para criar arquivos Parquet a partir dos dados
    def __private_CreateParquetFile(self, tipo:eTipo, anoInicio:int, anoTermino:int, df:any, nome_subtipo:str=None):
        """
        Cria e salva um arquivo Parquet a partir de um DataFrame.

        :param tipo: Tipo de negócio para nomear o arquivo.
        :param anoInicio: Ano inicial dos dados.
        :param anoTermino: Ano final dos dados.
        :param df: DataFrame contendo os dados a serem salvos.
        :param nome_subtipo: Nome do subtipo, se aplicável.
        """
        if tipo in (eTipo.PRODUCAO, eTipo.COMERCIALIZACAO):
            arquivo_parquet = f"data/parquet/bronze/{tipo.nome}_{anoInicio}{f'_{anoTermino}' if anoTermino and anoTermino != anoInicio else ''}.parquet"
        elif tipo in (eTipo.PROCESSAMENTO, eTipo.IMPORTACAO, eTipo.EXPORTACAO):
            arquivo_parquet = f"data/parquet/bronze/{tipo.nome}_{nome_subtipo}_{anoInicio}{f'_{anoTermino}' if anoTermino and anoTermino != anoInicio else ''}.parquet"
        df.to_parquet(arquivo_parquet, engine='pyarrow')  # Salva o DataFrame no formato Parquet

    # Função privada para realizar o scraping de uma página específica
    def __private_Scraping(self, codigo_tipo:int, nome_tipo:str, ano:int, codigo_subtipo:int=None, nome_subtipo:str=None, persistHtml:bool=False, persistParquet:bool=False):
        """
        Realiza o scraping de uma página específica e processa o conteúdo.

        :param codigo_tipo: Código do tipo de negócio para a URL.
        :param nome_tipo: Nome do tipo de negócio (ex.: Producao, Processamento).
        :param ano: Ano relacionado ao conteúdo.
        :param codigo_subtipo: Código do subtipo, se aplicável.
        :param nome_subtipo: Nome do subtipo, se aplicável.
        :param persistHtml: Define se o conteúdo HTML deve ser salvo.
        :param persistParquet: Define se os dados devem ser salvos em formato Parquet.
        :return: DataFrame processado a partir do HTML.
        """
        if nome_tipo in ("Producao", "Comercializacao"):
            url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{codigo_tipo}"  # URL para Producao e Comercializacao
        elif nome_tipo in ("Processamento", "Importacao", "Exportacao"):
            url = f"http://vitibrasil.cnpuv.embrapa.br/index.php?ano={ano}&opcao=opt_0{codigo_tipo}&subopcao=subopt_0{codigo_subtipo}"  # URL para Processamento, Importacao e Exportacao
        else:
            # Lança uma exceção HTTP se o tipo de negócio for inválido
            raise HTTPException(status_code=500, detail=f"tipo_negocio `{nome_tipo}` invalido")

        # Obtém o conteúdo HTML da URL usando uma função privada
        html_content = self.__private_getContent(url=url)

        # Salva o conteúdo HTML em um arquivo, se a opção persistHtml estiver ativada
        if persistHtml:
            self.__private_save_html_content(content=html_content, nome_tipo=nome_tipo, ano=ano, nome_subtipo=nome_subtipo)

        # Processa o conteúdo HTML e converte-o em um DataFrame
        df = self.__private_ProcessHtml(nome_tipo=nome_tipo, ano=ano, html_content=html_content)

        # Retorna o DataFrame processado
        return df


    # Função privada para processar o conteúdo HTML e extrair dados em um DataFrame
    def __private_ProcessHtml(self, nome_tipo:str, ano:int, html_content:str, nome_subtipo:str=None, persistParquet:bool=None):
        """
        Processa o conteúdo HTML para extrair dados e convertê-los em um DataFrame.

        Args:
            nome_tipo (str): Nome do tipo de negócio.
            ano (int): Ano relacionado ao conteúdo.
            html_content (str): Conteúdo HTML a ser processado.
            nome_subtipo (str, opcional): Nome do subtipo, se aplicável. Default é None.
            persistParquet (bool, opcional): Define se o DataFrame deve ser salvo em formato Parquet. Default é None.

        Returns:
            pd.DataFrame: DataFrame contendo os dados extraídos do HTML.
        """
        # Cria um parser HTML usando lxml
        parser = etree.HTMLParser()
        
        # Converte o conteúdo HTML em uma árvore de elementos
        tree = etree.fromstring(html_content, parser)
        
        # Seleciona as tabelas relevantes no HTML usando XPath
        table_elements = tree.xpath('//table[contains(@class, "tb_base") and contains(@class, "tb_dados")]')

        # Lista para armazenar os DataFrames extraídos das tabelas
        dfs = []

        # Itera sobre cada tabela encontrada no HTML
        for i, table in enumerate(table_elements):
            # Converte o HTML da tabela em uma string
            table_html = etree.tostring(table, pretty_print=True, encoding='unicode')
            
            # Converte a string HTML da tabela em um DataFrame usando pandas
            df = pd.read_html(StringIO(table_html))[0]
            
            # Adiciona o DataFrame à lista
            dfs.append(df)

        # Combina todos os DataFrames em um único DataFrame
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Retorna o DataFrame combinado
        return combined_df


    # Função pública para realizar o Web Scraping completo com as configurações fornecidas
    def WebScaping(self, tipo:eTipo=None, persistHtml:bool=False, persistParquet:bool=False, lerHTML_SemScraping:bool=True):
        """
        Realiza o Web Scraping completo baseado nas configurações fornecidas e no tipo de negócio.

        Args:
            tipo (eTipo, opcional): Tipo de negócio para o qual o scraping será realizado. Default é None.
            persistHtml (bool, opcional): Define se o conteúdo HTML deve ser salvo. Default é False.
            persistParquet (bool, opcional): Define se os dados devem ser salvos em formato Parquet. Default é False.
            lerHTML_SemScraping (bool, opcional): Define se o HTML deve ser lido diretamente sem scraping. Default é True.

        Returns:
            str: Dados processados em formato JSON ou realiza processamento adicional dependendo dos parâmetros.
        """
        # Define as variáveis de início e término do ano para o scraping
        anoInicio = self.anoInicio
        anoTermino = self.anoTermino
        tipo = self.tipo
        persistHtml = self.persistHtml
        persistParquet = self.persistParquet

        # Verifica se já existem arquivos prontos na camada Silver
        arquivos = retorna_arquivos(tipo=tipo, path='data/parquet/silver')
        if len(arquivos) == 1:
            print(f"Arquivo(s) na camada silver encontrado(s). Só pode existir 1 arquivo de cada tipo no formato parquet na pasta.")
            df = pd.read_parquet(arquivos[0])
            return df.to_json()

        # Verifica se já existem arquivos prontos na camada Bronze
        arquivos = retorna_arquivos(tipo=tipo, path='data/parquet/bronze')
        if len(arquivos) > 1:
            print(f"Arquivo(s) na camada bronze encontrado(s). Processando para gerar a camada Silver.")
            processa_bronze(tipo, 'data/parquet/bronze')
            return

        # Inicia o processo de Web Scraping para o tipo de negócio especificado
        print(f"*** INICIANDO WEBSCRAPING->{tipo.nome} -> Ano Inicio -> {anoInicio} Ano Termino {anoTermino}")

        df = None

        # Se o tipo de negócio for PRODUCAO ou COMERCIALIZACAO
        if tipo in (eTipo.PRODUCAO, eTipo.COMERCIALIZACAO):
            dfIteracoes = []
            # Itera sobre os anos especificados para o scraping
            for ano in range(anoInicio, anoTermino+1):
                print(f"****** Scraping: {tipo.nome} ano: {ano} até: {anoTermino}")
                with Negocio(tipo) as negocio:
                    # Realiza o scraping para o ano e tipo de negócio especificados
                    df = self.__private_Scraping(
                        codigo_tipo=negocio.codigo, 
                        nome_tipo=negocio.tipo, 
                        ano=ano, 
                        codigo_subtipo=None, 
                        nome_subtipo=None, 
                        persistHtml=persistHtml, 
                        persistParquet=persistParquet
                    )
                    if df is not None and len(df) > 0:
                        # Adiciona o ano e o tipo de negócio ao DataFrame
                        df["ano"] = ano
                        df["tipo"] = negocio.tipo
                        dfIteracoes.append(df)
            # Combina todos os DataFrames de iterações
            df = pd.concat(dfIteracoes, ignore_index=True)
            df = df.astype(str)  # Converte os dados para string
            if persistParquet:
                # Cria um arquivo Parquet com os dados processados
                self.__private_CreateParquetFile(
                    tipo=tipo, 
                    anoInicio=anoInicio, 
                    anoTermino=anoTermino, 
                    df=df, 
                    nome_subtipo=None
                )
                processa_bronze(tipo, 'data/parquet/bronze')

        # Se o tipo de negócio for PROCESSAMENTO, IMPORTACAO ou EXPORTACAO
        elif tipo in (eTipo.PROCESSAMENTO, eTipo.IMPORTACAO, eTipo.EXPORTACAO):
            with Negocio(tipo) as negocio:
                # Itera sobre os subtipos definidos no tipo de negócio
                for subtipo in negocio.subtipo.value:
                    dfIteracoes = []
                    # Itera sobre os anos especificados para o scraping
                    for ano in range(anoInicio, anoTermino+1):
                        print(f"****** Scraping:{tipo.nome}, {subtipo['nome']} ano: {ano} até: {anoTermino}")
                        # Realiza o scraping para o ano, tipo de negócio e subtipo especificados
                        df = self.__private_Scraping(
                            codigo_tipo=negocio.codigo, 
                            nome_tipo=negocio.tipo, 
                            ano=ano, 
                            codigo_subtipo=subtipo["codigo"], 
                            nome_subtipo=subtipo["nome"], 
                            persistHtml=persistHtml, 
                            persistParquet=persistParquet
                        )
                        if df is not None and len(df) > 0:
                            # Adiciona o ano, o tipo de negócio e o subtipo ao DataFrame
                            df["ano"] = ano
                            df["tipo"] = negocio.tipo
                            df["subtipo"] = subtipo["nome"]
                            dfIteracoes.append(df)
                    # Combina todos os DataFrames de iterações
                    df = pd.concat(dfIteracoes, ignore_index=True)
                    df = df.astype(str)  # Converte os dados para string
                    if persistParquet:
                        # Cria um arquivo Parquet com os dados processados
                        self.__private_CreateParquetFile(
                            tipo=tipo, 
                            anoInicio=anoInicio, 
                            anoTermino=anoTermino, 
                            df=df, 
                            nome_subtipo=subtipo["nome"]
                        )
                        processa_bronze(tipo, 'data/parquet/bronze')

        # Verifica novamente se o arquivo final Silver foi criado
        arquivos = retorna_arquivos(tipo=tipo, path='data/parquet/silver')
        if len(arquivos) == 1:
            print(f"Arquivo(s) na camada silver encontrado(s). Só pode existir 1 arquivo de cada tipo no formato parquet na pasta.")
            df = pd.read_parquet(arquivos[0])
            return df.to_json()  # Retorna o DataFrame como JSON