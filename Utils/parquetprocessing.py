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
from model.negocio import Negocio, eTipo
import glob
from pathlib import Path
from enum import Enum

def retorna_arquivos(tipo:eTipo, path:str):
    # Padrão de arquivo com máscara
    padrao = f"{tipo.nome}*.parquet"
    print(f"padrao->{padrao}")
    print(f"arquivo->{str(Path(path))}")
    # Buscar arquivos que correspondem ao padrão
    arquivos = glob.glob(str(Path(path) / padrao))
    print(f"arquivos->{arquivos}")
    # Verificar se algum arquivo foi encontrado
    return arquivos

from decimal import Decimal, InvalidOperation
def converter_numero_ingles_para_brasileiro_decimal(numero_str):
    try:
         # Remover o separador de milhar
        numero_str = numero_str.replace('.', '')
        # Substituir o separador decimal
        numero_str = numero_str.replace(',', '.')
        # Converter para Decimal
        return pd.to_numeric(numero_str, errors='coerce')
    except (ValueError, InvalidOperation):
        return None
    
def processa_bronze(tipo:eTipo, path:str):

    arquivos = retorna_arquivos(tipo, path)

    if arquivos:
        if (tipo in (eTipo.PRODUCAO, eTipo.COMERCIALIZACAO)):
            df = pd.read_parquet(arquivos[0])
            df_tipado = df.copy()
            df_agrupado = df_tipado.copy()
            df_detalhes = df_tipado.copy()

            # normalizar quantidade
            df_tipado["Quantidade (L.)"] = df_tipado["Quantidade (L.)"].apply(converter_numero_ingles_para_brasileiro_decimal)
            print(f"df_tipado->{df_tipado}")

            # Filtrar os dados para Produto='Total'
            df_total_ano = df_tipado[df_tipado['Produto'] == 'Total'].copy()
            print(f"df_total_ano->{df_total_ano}")
            df_total_ano = df_total_ano.groupby('ano')['Quantidade (L.)'].sum().reset_index()

            df_agrupado = df_agrupado[df_agrupado["Produto"].str.isupper()]
            df_agregado_ano = df_agrupado.groupby(['ano', 'Produto'], as_index=False).agg({'Quantidade (L.)': 'sum'})
            print(f"df_agregado_ano->{df_agregado_ano}")
            df_agregado_ano.to_parquet(f'../data/parquet/silver/{tipo.nome}_AGREGADO_ANO_CATEGORIA.parquet')

            df_detalhes['categoria'] = df_detalhes['Produto'].where(df_detalhes['Produto'].str.isupper()).ffill()
            df_detalhes = df_detalhes[~df_detalhes['Produto'].str.isupper()]
            df_detalhes = df_detalhes[df_detalhes['Produto'] != 'Total']

            print(f"df_detalhes->{df_detalhes}")
        elif(tipo == eTipo.PROCESSAMENTO):
            dfs = []
            for arquivo in arquivos:
                print(f'processando arquivo: {arquivo}')
                df = pd.read_parquet(arquivo)
                df.rename(columns={"Sem definição":"Cultivar"}, inplace=True)
                dfs.append(df)

            df = pd.concat(dfs, ignore_index=True)

            df_tipado = df.copy()
            df_agrupado = df_tipado.copy()
            df_detalhes = df_tipado.copy()

            df_tipado["Quantidade (Kg)"] = df_tipado["Quantidade (Kg)"].apply(converter_numero_ingles_para_brasileiro_decimal)
            print(f"df_tipado->{df_tipado}")
            #if (tipo.subtipo.value["nome"] != "Sem Classificação"):
                # Filtrar os dados para Produto='Total'
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
            #else:
                # Filtrar os dados para Produto='Total'
                #df_total_ano = df_tipado[df_tipado['Sem definição'] == 'Total'].copy()
                #print(f"df_total_ano->{df_total_ano}")
        elif (tipo in (eTipo.IMPORTACAO, eTipo.EXPORTACAO)):
            df = pd.read_parquet(arquivos[0])
            df_tipado = df.copy()
            df_agrupado = df_tipado.copy()
            df_detalhes = df_tipado.copy()

            df_total_ano = df_tipado[df_tipado['Países'] == 'Total'].copy()
            df_total_ano = df_total_ano.groupby('ano')[['Quantidade (Kg)', 'Valor (US$)']].sum().reset_index()
            print(f"df_total_ano->{df_total_ano}")

            df_tipado["Quantidade (Kg)"] = df_tipado["Quantidade (Kg)"].apply(converter_numero_ingles_para_brasileiro_decimal)
            df_tipado["Valor (US$)"] = df_tipado["Valor (US$)"].apply(converter_numero_ingles_para_brasileiro_decimal)            
            print(f"df_tipado->{df_tipado}")
            
            df_detalhes = df_detalhes[df_detalhes['Países'] != 'Total']
            print(f"df_detalhes->{df_detalhes}")

        # Somar a quantidade por ano
        df_total_ano.to_parquet(f'../data/parquet/silver/{tipo.nome}_AGREGADO_ANO.parquet')
        df_detalhes.to_parquet(f'../data/parquet/silver/{tipo.nome}.parquet')