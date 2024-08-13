import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO
import jwt

# Definindo anos padrão
ANO_INICIAL = 1970
ANO_FINAL = 2024

# Função para conectar ao banco de dados PostgreSQL e obter os dados
def obter_dados_do_banco(categoria, subcategorias, ano_inicio, ano_fim):
    try:
        dfs_mapa = []
        df = pd.read_parquet(f"data\parquet\silver\Importacao.parquet")
        df['ano'] = pd.to_numeric(df['ano'])
        df_mapa = df[df['ano'].between(ano_inicio, ano_fim)]
        
        # Convertendo valores para numérico
        df_mapa.rename(columns={'Valor (US$)': 'valor'}, inplace=True)
        df_mapa['valor'] = pd.to_numeric(df_mapa['valor'], errors='coerce')
        df_mapa = df_mapa[df_mapa['valor'] > 0]  # Filtrando valores maiores que 0
        
        dfs_mapa.append(df_mapa)
        
        df_mapa = pd.concat(dfs_mapa, ignore_index=True)
        return df_mapa
    except Exception as e:
        st.error(f"Erro ao conectar ao banco de dados: {e}")
        return pd.DataFrame()

# Função para criar os cards
def criar_cards(df, nome_categoria):
    if df.empty:
        return [None, None, None, None]

    # Total de registros
    quantidade_total = df.shape[0]
    # Total de valor
    valor_total = df['valor'].sum()
    df = df.rename(columns={'Países':"pais"})
    # Média de países por ano
    media_paises_por_ano = df.groupby('ano')['pais'].nunique().mean()
    
    # Quantidade de países distintos no período
    quantidade_paises_distintos = df['pais'].nunique()
    
    # Três países com maior quantidade
    top_3_paises_quantidade = df.groupby('pais').size().nlargest(3)
    info_top_3_paises_quantidade = '\n'.join(f"- {pais}: {quantidade}" for pais, quantidade in top_3_paises_quantidade.items())
    
    # Três países com maior valor
    top_3_paises_valor = df.groupby('pais')['valor'].sum().nlargest(3)
    info_top_3_paises_valor = '\n'.join(f"- {pais}: R${valor:,.2f}" for pais, valor in top_3_paises_valor.items())
    
    cards = {
        'Quantidade Total': f'{quantidade_total} ({nome_categoria})',
        'Valor Total': f'R${valor_total:,.2f}',
        'Média de Países por Ano': f'{media_paises_por_ano:.2f}',
        'Quantidade de Países': f'{quantidade_paises_distintos}',
        'Top 3 Países com Maior Quantidade': info_top_3_paises_quantidade,
        'Top 3 Países com Maior Valor': info_top_3_paises_valor
    }
    
    return cards

# Função para criar o gráfico de barras tipo Pareto
def criar_grafico_barras(df):
    if df.empty:
        st.warning('Nenhum dado disponível para o gráfico de barras.')
        return None
    
    # Agregando dados para o gráfico
    df_agregado = df.groupby('pais').agg({'valor': 'sum'}).reset_index()
    df_agregado['quantidade'] = df.groupby('pais').size().values
    
    # Selecionando os 5 países com maiores valores
    top_5_paises = df_agregado.nlargest(5, 'valor')
    
    # Ordenar os países pelo valor
    top_5_paises = top_5_paises.sort_values(by='valor', ascending=False)
    top_5_paises['percentual_acumulado'] = top_5_paises['valor'].cumsum() / top_5_paises['valor'].sum() * 100
    
    plt.figure(figsize=(14, 8))
    plt.style.use('dark_background')
    
    fig, ax1 = plt.subplots(figsize=(14, 8))
    
    # Gráfico de barras
    cor = 'tab:blue'
    ax1.set_xlabel('País')
    ax1.set_ylabel('Quantidade', color=cor)
    sns.barplot(data=top_5_paises, x='pais', y='quantidade', color=cor, ax=ax1)
    ax1.tick_params(axis='y', labelcolor=cor)
    
    # Gráfico de linha (percentual acumulado)
    ax2 = ax1.twinx()
    cor = 'tab:red'
    ax2.set_ylabel('Valor (R$)', color=cor)
    sns.lineplot(data=top_5_paises, x='pais', y='valor', color=cor, marker='o', ax=ax2)
    ax2.tick_params(axis='y', labelcolor=cor)
    ax2.set_ylim(0, top_5_paises['valor'].max() * 1.1)
    
    # Configurando o gráfico
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('white')
    ax1.spines['bottom'].set_color('white')
    ax1.xaxis.label.set_color('white')
    ax1.yaxis.label.set_color('white')
    ax1.tick_params(axis='both', colors='white')
    
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('white')
    ax2.spines['bottom'].set_color('white')
    ax2.yaxis.label.set_color('white')
    ax2.tick_params(axis='both', colors='white')
    
    buf = BytesIO()
    plt.savefig(buf, format='png', bbox_inches='tight', dpi=300, transparent=True)
    buf.seek(0)
    return buf

# Função para mostrar o dashboard
def painel_dashboard():
    st.set_page_config(page_title='Embrapa Dashboard', page_icon='📊', layout='wide')
    st.title('Dashboard Interativo - Análise de Importação e Exportação de Produtos de Bebidas (Embrapa)')
    
    st.sidebar.header('Filtros de Ano')
    ano_inicio = st.sidebar.number_input('Ano Inicial', min_value=ANO_INICIAL, max_value=ANO_FINAL, value=2000, step=1)
    ano_fim = st.sidebar.number_input('Ano Final', min_value=ANO_INICIAL, max_value=ANO_FINAL, value=2024, step=1)
    
    if ano_inicio > ano_fim:
        st.sidebar.error('O ano inicial deve ser menor ou igual ao ano final.')
        return
    
    st.sidebar.header('Escolha a Categoria')
    categoria = st.sidebar.radio('Categoria', ['Importação'])

    st.sidebar.header('Escolha os Tipos de Produtos')
    subcategorias = st.sidebar.multiselect(
        'Subcategorias',
        ['Espumantes', 'Suco de Uvas', 'Uvas Frescas', 'Vinhos de Mesa'],
        default=['Espumantes']  # Definindo uma opção padrão
    )
    
    if not subcategorias:
        st.sidebar.error('Selecione pelo menos uma subcategoria.')
        return
    
    # Ajustando os nomes das subcategorias para consulta
    mapa_categorias = {'Exportação': 'export', 'Importação': 'import'}
    mapa_subcategorias = {'Espumantes': 'espumantes', 'Suco de Uvas': 'suco_de_uvas', 'Uvas Frescas': 'uvas_frescas', 'Vinhos de Mesa': 'vinhos_de_mesa'}
    
    df_mapa = obter_dados_do_banco(mapa_categorias[categoria], [mapa_subcategorias[subcat] for subcat in subcategorias], ano_inicio, ano_fim)
    if df_mapa.empty:
        st.warning('Nenhum dado encontrado para o intervalo de anos e filtros selecionados.')
        return
    
    # Painel de Informações Resumidas
    st.subheader('Informações Resumidas')
    st.write(f"**Período de Análise:** {ano_inicio} a {ano_fim}")
    st.write(f"**Categoria Selecionada:** {categoria}")
    st.write(f"**Subcategorias Selecionadas:** {', '.join(subcategorias)}")
    
    cards = criar_cards(df_mapa, categoria)
    col1, col2 = st.columns([1, 4])
    
    with col1:
        st.metric(label=f'Quantidade Total de {categoria} no período', value=cards['Quantidade Total'])
        st.metric(label='Valor Total', value=cards['Valor Total'])
        st.metric(label='Média de Países por Ano', value=cards['Média de Países por Ano'])
        st.metric(label='Quantidade de Países', value=cards['Quantidade de Países'])
        st.write(f"**Top 3 Países com Maior Valor:**\n{cards['Top 3 Países com Maior Valor']}")
    
    with col2:
        # Painel do Mapa Interativo
        st.subheader('Mapa Interativo')
        df_mapa.rename(columns={'Países':"pais"}, inplace=True)
        df_paises = pd.read_csv(f"data/paises.csv", sep=";")
        df_paises.rename(columns={'nome':'pais'}, inplace=True)
        df_mapa_paises = pd.merge(df_mapa, df_paises, on='pais', how='left')
        
        df_mapa_paises = df_mapa_paises.dropna(subset=['latitude', 'longitude'])
        df_mapa_paises['latitude'] = pd.to_numeric(df_mapa_paises['latitude'].str.replace(',', '.'))
        df_mapa_paises['longitude'] = pd.to_numeric(df_mapa_paises['longitude'].str.replace(',', '.'))

        if not df_mapa_paises.empty:
            st.map(df_mapa_paises[['latitude', 'longitude']])
    
    col1, col2 = st.columns([1, 4])

    with col1:  
        # Painel de Análise por Categoria e Subcategoria
        st.subheader('Análise por Categoria e Subcategoria')
        st.write(df_mapa_paises.drop(columns=['latitude', 'longitude']))
    
    with col2:
        # Painel do Gráfico de Barras Tipo Pareto
        st.subheader('Gráfico de Barras Tipo Pareto')
        grafico_buf = criar_grafico_barras(df_mapa_paises)
        if grafico_buf:
            st.image(grafico_buf)

# Executa o dashboard
if __name__ == '__main__':

        painel_dashboard()
