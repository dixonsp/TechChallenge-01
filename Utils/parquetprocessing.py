# Importação das bibliotecas necessárias para o Web Scraping e manipulação de dados

import streamlit as st  # Biblioteca para criar interfaces de usuário interativas
from selenium import webdriver  # Módulo webdriver para realizar web scraping
from selenium.webdriver.chrome.options import Options  # Módulo Options para definição de parâmetros de execução do navegador Chrome
from fastapi import HTTPException  # Módulo para o tratamento de exceções HTTP
import json  # Módulo para manipulação de dados JSON
from selenium.webdriver.common.by import By  # Módulo By que traz alguns parâmetros para navegação durante o scraping
import time  # Módulo time para trabalhar com tempos de espera e delays
from datetime import datetime  # Módulo datetime para manipulação de datas e horários
import pandas as pd  # Biblioteca pandas para manipulação e análise de dados estruturados
from lxml import etree  # Biblioteca lxml para trabalhar com conteúdo HTML e XML
from io import StringIO  # Módulo io para manipulação de fluxos de entrada e saída de texto
import pyarrow  # Biblioteca para trabalhar com arquivos Parquet e colunar
import os  # Módulo para interagir com o sistema de arquivos
import sys  # Módulo para manipulação de operações relacionadas ao sistema
from model.negocio import Negocio, eTipo  # Importa classes personalizadas para manipulação de negócios e tipos de negócios
import glob  # Módulo para buscar arquivos no sistema de arquivos usando padrões
from pathlib import Path  # Módulo Path para trabalhar com caminhos de arquivos e diretórios
from enum import Enum  # Módulo Enum para criar enumeradores em Python

# Função para retornar os arquivos que correspondem a um padrão em um diretório específico
def retorna_arquivos(tipo: eTipo, path: str):
    """
    Retorna uma lista de arquivos que correspondem ao padrão de nome especificado para o tipo de negócio.

    Args:
        tipo (eTipo): Tipo de negócio para o qual os arquivos serão buscados.
        path (str): Caminho do diretório onde os arquivos serão buscados.

    Returns:
        list: Lista de arquivos que correspondem ao padrão de nome.
    """
    # Define o padrão de nome de arquivo usando o tipo de negócio
    padrao = f"{tipo.nome}*.parquet"
    print(f"padrao->{padrao}")
    print(f"arquivo->{str(Path(path))}")
    
    # Busca os arquivos que correspondem ao padrão
    arquivos = glob.glob(str(Path(path) / padrao))
    print(f"arquivos->{arquivos}")
    
    # Retorna a lista de arquivos encontrados
    return arquivos

# Função para converter números do formato inglês para o formato brasileiro e retornar como Decimal
from decimal import Decimal, InvalidOperation
def converter_numero_ingles_para_brasileiro_decimal(numero_str):
    """
    Converte um número no formato inglês para o formato brasileiro e retorna como Decimal.

    Args:
        numero_str (str): Número como string no formato inglês (com ponto como separador de milhar e vírgula como separador decimal).

    Returns:
        Decimal: Número convertido para o formato brasileiro, ou None em caso de erro.
    """
    try:
        # Remove o separador de milhar
        numero_str = numero_str.replace('.', '')
        
        # Substitui o separador decimal
        numero_str = numero_str.replace(',', '.')
        
        # Converte a string para Decimal
        return pd.to_numeric(numero_str, errors='coerce')
    except (ValueError, InvalidOperation):
        # Retorna None se ocorrer um erro na conversão
        return None

# Função para processar dados na camada Bronze e gerar a camada Silver
def processa_bronze(tipo: eTipo, path: str):
    """
    Processa os arquivos da camada Bronze e gera a camada Silver para um tipo de negócio específico.

    Args:
        tipo (eTipo): Tipo de negócio que está sendo processado.
        path (str): Caminho do diretório onde os arquivos Bronze estão armazenados.
    """
    # Obtém os arquivos da camada Bronze para o tipo de negócio
    arquivos = retorna_arquivos(tipo, path)

    # Se houver arquivos encontrados
    if arquivos:
        # Processamento para os tipos de negócio "PRODUCAO" e "COMERCIALIZACAO"
        if tipo in (eTipo.PRODUCAO, eTipo.COMERCIALIZACAO):
            df = pd.read_parquet(arquivos[0])  # Lê o primeiro arquivo Parquet encontrado
            df_tipado = df.copy()  # Cria uma cópia do DataFrame original
            df_agrupado = df_tipado.copy()  # Cria outra cópia para agrupamento
            df_detalhes = df_tipado.copy()  # Cria outra cópia para detalhes

            # Normaliza a quantidade no formato brasileiro
            df_tipado["Quantidade (L.)"] = df_tipado["Quantidade (L.)"].apply(converter_numero_ingles_para_brasileiro_decimal)
            print(f"df_tipado->{df_tipado}")

            # Filtra os dados para Produto='Total'
            df_total_ano = df_tipado[df_tipado['Produto'] == 'Total'].copy()
            print(f"df_total_ano->{df_total_ano}")
            df_total_ano = df_total_ano.groupby('ano')['Quantidade (L.)'].sum().reset_index()

            # Filtra os dados para produtos com letras maiúsculas
            df_agrupado = df_agrupado[df_agrupado["Produto"].str.isupper()]
            df_agregado_ano = df_agrupado.groupby(['ano', 'Produto'], as_index=False).agg({'Quantidade (L.)': 'sum'})
            print(f"df_agregado_ano->{df_agregado_ano}")
            df_agregado_ano.to_parquet(f'../data/parquet/silver/{tipo.nome}_AGREGADO_ANO_CATEGORIA.parquet')

            # Preenche categorias e filtra produtos que não são maiúsculas
            df_detalhes['categoria'] = df_detalhes['Produto'].where(df_detalhes['Produto'].str.isupper()).ffill()
            df_detalhes = df_detalhes[~df_detalhes['Produto'].str.isupper()]
            df_detalhes = df_detalhes[df_detalhes['Produto'] != 'Total']
            print(f"df_detalhes->{df_detalhes}")

        # Processamento para o tipo de negócio "PROCESSAMENTO"
        elif tipo == eTipo.PROCESSAMENTO:
            dfs = []
            for arquivo in arquivos:
                print(f'processando arquivo: {arquivo}')
                df = pd.read_parquet(arquivo)
                df.rename(columns={"Sem definição": "Cultivar"}, inplace=True)
                dfs.append(df)

            df = pd.concat(dfs, ignore_index=True)

            df_tipado = df.copy()
            df_agrupado = df_tipado.copy()
            df_detalhes = df_tipado.copy()

            df_tipado["Quantidade (Kg)"] = df_tipado["Quantidade (Kg)"].apply(converter_numero_ingles_para_brasileiro_decimal)
            print(f"df_tipado->{df_tipado}")

            df_total_ano = df_tipado[df_tipado['Cultivar'] == 'Total'].copy()
            df_total_ano = df_total_ano.groupby('ano')['Quantidade (Kg)'].sum().reset_index()
            print(f"df_total_ano->{df_total_ano}")

            df_agrupado = df_agrupado[df_agrupado["Cultivar"].str.isupper()]
            df_agregado_ano = df_agrupado.groupby(['ano', 'Cultivar'], as_index=False).agg({'Quantidade (Kg)': 'sum'})
            print(f"df_agregado_ano->{df_agregado_ano}")
            df_agregado_ano.to_parquet(f'../data/parquet/silver/{tipo.nome}_AGREGADO_ANO_CATEGORIA.parquet')

            df_detalhes['categoria'] = df_detalhes['Cultivar'].where(df_detalhes['Cultivar'].str.isupper()).ffill()
            df_detalhes = df_detalhes[~df_detalhes['Cultivar'].str.isupper()]
            df_detalhes = df_detalhes[df_detalhes['Cultivar'] != 'Total']
            print(f"df_detalhes->{df_detalhes}")

        # Processamento para os tipos de negócio "IMPORTACAO" e "EXPORTACAO"
        elif tipo in (eTipo.IMPORTACAO, eTipo.EXPORTACAO):
            df = pd.read_parquet(arquivos[0])  # Lê o primeiro arquivo Parquet encontrado
            df_tipado = df.copy()  # Cria uma cópia do DataFrame original
            df_agrupado = df_tipado.copy()  # Cria outra cópia para agrupamento
            df_detalhes = df_tipado.copy()  # Cria outra cópia para detalhes

            df_total_ano = df_tipado[df_tipado['Países'] == 'Total'].copy()
            df_total_ano = df_total_ano.groupby('ano')[['Quantidade (Kg)', 'Valor (US$)']].sum().reset_index()
            print(f"df_total_ano->{df_total_ano}")

            # Converte os valores na coluna "Quantidade (Kg)" do formato inglês para o formato brasileiro usando a função personalizada
            df_tipado["Quantidade (Kg)"] = df_tipado["Quantidade (Kg)"].apply(converter_numero_ingles_para_brasileiro_decimal)

            # Converte os valores na coluna "Valor (US$)" do formato inglês para o formato brasileiro usando a mesma função
            df_tipado["Valor (US$)"] = df_tipado["Valor (US$)"].apply(converter_numero_ingles_para_brasileiro_decimal)

            # Exibe o DataFrame tipado no console para verificação
            print(f"df_tipado->{df_tipado}")

            # Filtra os detalhes removendo as linhas onde o valor na coluna 'Países' é 'Total'
            df_detalhes = df_detalhes[df_detalhes['Países'] != 'Total']

            # Exibe o DataFrame de detalhes no console para verificação
            print(f"df_detalhes->{df_detalhes}")

            # Salva o DataFrame agregado por ano em um arquivo Parquet na camada Silver
            df_total_ano.to_parquet(f'../data/parquet/silver/{tipo.nome}_AGREGADO_ANO.parquet')

            # Salva o DataFrame de detalhes em um arquivo Parquet na camada Silver
            df_detalhes.to_parquet(f'../data/parquet/silver/{tipo.nome}.parquet')