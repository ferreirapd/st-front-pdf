import streamlit as st
import pandas as pd
import numpy as np
import boto3
import io
import os
import unidecode
import re
import plotly.graph_objects as go
import locale
from scipy import stats
from scipy.stats import gaussian_kde
from scipy.signal import savgol_filter
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()

# Criar função para ler arquivos parquet
def get_s3_client() -> boto3.client:
    """
    Cria e retorna um cliente S3 usando as credenciais configuradas.

    :return s3_client: Retorna o cliente S3 criado
    """
    try:
        aws_access_key = st.secrets.get("AWS_ACCESS_KEY_ID")
        aws_secret_key = st.secrets.get("AWS_SECRET_ACCESS_KEY")

        if not aws_access_key or not aws_secret_key:
            aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
            aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name='us-east-1'
        )

        return s3_client

    except Exception as e:
        print(f"Erro ao criar cliente S3: {str(e)}")
        raise


# Criar função para ler parquets e transformar em dataframe
def read_parquet_files_eda() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lê os arquivos parquet específicos da pasta input do bucket
    e retorna três dataframes: clientes, navegacao e transacao.

    :return df_clientes: Dataframe com os dados dos clientes
    :return df_navegacao: Dataframe com os dados de navegação
    :return df_transacao: Dataframe com os dados de transações
    """
    # Configurações do bucket
    bucket_name = 'bkt-dev-projcdia-rennerrethink-streamlit'
    input_prefix = 'input/'

    # Obtém o cliente S3
    s3_client = get_s3_client()

    # Inicializa os dataframes
    df_clientes = pd.DataFrame()
    df_navegacao = pd.DataFrame()
    df_transacao = pd.DataFrame()

    try:
        # Lista todos os objetos na pasta input
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=input_prefix
        )

        # Verifica se existem objetos
        if 'Contents' not in response:
            print(f"Nenhum arquivo encontrado em {input_prefix}")

            return df_clientes, df_navegacao, df_transacao

        # Para cada arquivo na pasta input
        for obj in response['Contents']:
            file_key = obj['Key']

            # Verifica se é um arquivo parquet
            if file_key.endswith('.parquet'):
                try:
                    # Lê o arquivo parquet
                    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                    file_name = file_key.split('/')[-1].lower()

                    if 'cliente' in file_name:
                        df_clientes = pd.read_parquet(io.BytesIO(response['Body'].read()))
                        print("Arquivo de clientes lido com sucesso!")
                    elif 'navegacao' in file_name:
                        df_navegacao = pd.read_parquet(io.BytesIO(response['Body'].read()))
                        print("Arquivo de navegação lido com sucesso!")
                    elif 'transacao' in file_name:
                        df_transacao = pd.read_parquet(io.BytesIO(response['Body'].read()))
                        print("Arquivo de transações lido com sucesso!")

                except Exception as e:
                    print(f"Erro ao ler arquivo {file_key}: {str(e)}")
                    continue

        return df_clientes, df_navegacao, df_transacao

    except Exception as e:
        print(f"Erro ao listar objetos do bucket: {str(e)}")
        raise


# Criar função para ler csvs e transformar em dataframe
def read_csv_files_eda() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lê os arquivos CSV específicos da pasta input do bucket
    e retorna três dataframes: clientes, navegacao e transacao.

    :return df_clientes: Dataframe com os dados dos clientes
    :return df_navegacao: Dataframe com os dados de navegação
    :return df_transacao: Dataframe com os dados de transações
    """
    # Configurações do bucket
    bucket_name = 'bkt-dev-projcdia-rennerrethink-streamlit'
    input_prefix = 'input/'

    # Obtém o cliente S3
    s3_client = get_s3_client()

    # Inicializa os dataframes
    df_clientes = pd.DataFrame()
    df_navegacao = pd.DataFrame()
    df_transacao = pd.DataFrame()

    try:
        # Lista todos os objetos na pasta input
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=input_prefix
        )

        # Verifica se existem objetos
        if 'Contents' not in response:
            print(f"Nenhum arquivo encontrado em {input_prefix}")
            return df_clientes, df_navegacao, df_transacao

        # Para cada arquivo na pasta input
        for obj in response['Contents']:
            file_key = obj['Key']

            # Verifica se é um arquivo CSV
            if file_key.endswith('.csv'):
                try:
                    # Lê o arquivo CSV
                    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                    file_name = file_key.split('/')[-1].lower()

                    # Usa read_csv para ler o conteúdo do arquivo
                    if 'cliente' in file_name:
                        df_clientes = pd.read_csv(io.BytesIO(response['Body'].read()))
                        print("Arquivo de clientes lido com sucesso!")
                    elif 'navegacao' in file_name:
                        df_navegacao = pd.read_csv(io.BytesIO(response['Body'].read()))
                        print("Arquivo de navegação lido com sucesso!")
                    elif 'transacao' in file_name:
                        df_transacao = pd.read_csv(io.BytesIO(response['Body'].read()))
                        print("Arquivo de transações lido com sucesso!")

                except Exception as e:
                    print(f"Erro ao ler arquivo {file_key}: {str(e)}")
                    continue

        return df_clientes, df_navegacao, df_transacao

    except Exception as e:
        print(f"Erro ao listar objetos do bucket: {str(e)}")
        raise


def converte_data_clientes(df_clientes: pd.DataFrame) -> pd.DataFrame:
    """
    Converte as colunas de datas do dataframe de clientes para o tipo datetime.

    :param df_clientes: Dataframe com os dados dos clientes
    :return df_clientes: Dataframe com a coluna data_nascimento convertida para datetime
    """
    # Conversão das datas
    df_clientes['data_ultima_compra_renner'] = pd.to_datetime(df_clientes['data_ultima_compra_renner']).dt.date
    df_clientes['data_primeira_compra_renner'] = pd.to_datetime(df_clientes['data_primeira_compra_renner']).dt.date

    return df_clientes


def limpar_nomes_cidades(cidade: str) -> str:
    """
    Limpa o nome da cidade, removendo acentos, caracteres especiais e espaços extras.

    :param cidade: Nome da cidade a ser limpo
    :return cidade_limpa: Nome da cidade limpo
    """
    substituicoes_caracteres = {'£': 'A'}
    cidade_tratada = str(cidade).upper()

    for caractere_especial, substituicao in substituicoes_caracteres.items():
        cidade_tratada = cidade_tratada.replace(caractere_especial, substituicao)

    cidade_sem_acento = unidecode.unidecode(cidade_tratada)
    cidade_limpa = re.sub(r'[^A-Z\s]', '', cidade_sem_acento)
    cidade_limpa = ' '.join(cidade_limpa.split())
    cidade_limpa = cidade_limpa.replace('SAAO', 'SAO')

    return cidade_limpa


def aplicar_limpeza_cidades(df_clientes: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica a limpeza de nomes de cidades no dataframe de clientes.

    :param df_clientes: Dataframe com os dados dos clientes
    :return df_clientes: Dataframe com a coluna cidade limpa
    """
    # Aplicar a limpeza
    df_clientes['cidade'] = df_clientes['cidade'].apply(limpar_nomes_cidades)

    # Lista de todas as capitais brasileiras, incluindo o Distrito Federal, em maiúscula e sem acentuação
    capitais = [
        'ARACAJU', 'BELEM', 'BELO HORIZONTE', 'BOA VISTA', 'BRASILIA', 'CAMPO GRANDE', 
        'CUIABA', 'CURITIBA', 'FLORIANOPOLIS', 'FORTALEZA', 'GOIANIA', 'JOAO PESSOA', 
        'MACAPA', 'MACEIO', 'MANAUS', 'NATAL', 'PALMAS', 'PORTO ALEGRE', 'PORTO VELHO', 
        'RECIFE', 'RIO BRANCO', 'RIO DE JANEIRO', 'SALVADOR', 'SAO LUIS', 'SAO PAULO', 
        'TERESINA', 'VITORIA'
    ]

    # Adicionar coluna "capital" com a informação 1 quando Capital ou 0 quando outro
    df_clientes['capital'] = df_clientes['cidade'].apply(lambda x: 1 if x in capitais else 0)

    # Mapear os valores 0 e 1 para "Interior" e "Capital"
    df_clientes['capital_label'] = df_clientes['capital'].map({1: 'Capital', 0: 'Interior'})

    return df_clientes


def grafico_capitais_interior(df_clientes: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de barras interativo mostrando a distribuição de clientes
    entre capitais e interior.

    :param df_clientes: DataFrame contendo a coluna 'capital_label'
    :return fig: Figura do Plotly pronta para ser exibida
    """
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')

    # Calcular as contagens e percentuais
    contagem = df_clientes['capital_label'].value_counts()
    percentuais = (contagem / len(df_clientes) * 100).round(1)

    valores_formatados = ['{:n}'.format(int(valor)) for valor in contagem.values]

    # Criar o gráfico usando Plotly
    fig = go.Figure(data=[
        go.Bar(
            x=contagem.index,
            y=contagem.values,
            text=valores_formatados,
            textposition='inside',
            textfont=dict(size=14),
            marker_color='red'
        )
    ])

    for i, (perc, valor) in enumerate(zip(percentuais, contagem.values)):
        fig.add_annotation(
            x=contagem.index[i],
            y=valor,
            text=f'{perc:.1f}%',
            showarrow=False,
            yshift=15,  # Ajuste a distância do texto em relação à barra
            font=dict(size=18)  # Fonte maior para os percentuais
        )

    # Personalizar o layout
    fig.update_layout(
        title={
            'text': 'Contagem de Clientes de Capitais e Interior',
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title="",
        yaxis_title="",  # Removido título do eixo y
        showlegend=False,
        xaxis={
            'categoryorder':'total ascending',  # Ordena as barras por valor
            'tickfont': {'size': 18}  # Aumenta o tamanho da fonte das labels do eixo x
        },
        plot_bgcolor='white',
        height=500,
        width=800,
        margin=dict(t=100, l=40, r=40, b=40)  # Reduzida margem esquerda
    )

    # Remover grid e linha do eixo y
    fig.update_yaxes(
        showgrid=False,
        showline=False,
        showticklabels=False  # Remove os números do eixo y
    )

    # Remover linha do eixo x
    fig.update_xaxes(
        showline=False,
        showgrid=False
    )

    return fig


def criar_grafico_distribuicao_idade(df_clientes):
    """
    Cria um histograma interativo mostrando a distribuição de idade dos clientes
    com linha de densidade suave.
    
    Args:
        df_clientes (pd.DataFrame): DataFrame contendo a coluna 'idade'
        
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    # Definir os bins manualmente
    min_idade = df_clientes['idade'].min()
    max_idade = df_clientes['idade'].max()

    # Criar o histograma
    fig = go.Figure()
    
    # Adicionar o histograma com divisões visíveis entre as barras
    fig.add_trace(go.Histogram(
        x=df_clientes['idade'],
        xbins=dict(
            start=min_idade,
            end=max_idade,
            size=(max_idade - min_idade) / 10
        ),
        name='Histograma',
        marker=dict(
            color='red',
            line=dict(
                color='white',
                width=1
            )
        ),
    ))
    
    # Calcular KDE usando scipy para uma curva mais suave
    kde = stats.gaussian_kde(df_clientes['idade'].dropna())
    x_range = np.linspace(min_idade, max_idade, 200)
    kde_y = kde(x_range) * len(df_clientes['idade']) * (max_idade - min_idade) / 10
    
    # Adicionar a linha de densidade
    fig.add_trace(go.Scatter(
        x=x_range,
        y=kde_y,
        mode='lines',
        name='Densidade',
        line=dict(color='black', width=2),
        yaxis='y'
    ))
    
    # Personalizar o layout
    fig.update_layout(
        title={
            'text': 'Distribuição de Idade dos Clientes',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title='Idade',
        yaxis_title='Número de Clientes',
        xaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16}
        },
        yaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16},
            'showgrid': False,
            'showline': False
        },
        plot_bgcolor='white',
        height=500,
        width=800,
        showlegend=False,
        margin=dict(t=100, l=70, r=40, b=60),
        bargap=0  # Remove o espaço entre as barras
    )
    
    # Remover linhas de grade e bordas
    fig.update_xaxes(showgrid=False, showline=False)
    
    return fig


def criar_grafico_distribuicao_idades_negativas(df_clientes):
    """
    Cria um histograma interativo mostrando a distribuição de idades negativas dos clientes.
    Também retorna o número total de clientes com idade negativa.
    
    Args:
        df_clientes (pd.DataFrame): DataFrame contendo a coluna 'idade'
        
    Returns:
        tuple: (figura do Plotly, número de clientes com idade negativa)
    """
    # Filtrar idades negativas
    df_negativos = df_clientes[df_clientes['idade'] < 0]
    total_negativos = len(df_negativos)
    
    # Criar o histograma
    fig = go.Figure()
    
    # Definir os bins
    min_idade = df_negativos['idade'].min()
    max_idade = df_negativos['idade'].max()
    
    # Adicionar o histograma
    fig.add_trace(go.Histogram(
        x=df_negativos['idade'],
        name='Histograma',
        marker=dict(
            color='red',
            line=dict(
                color='white',
                width=1
            )
        ),
    ))
    
    # Calcular KDE usando scipy para uma curva mais suave
    if len(df_negativos) > 1:  # Só calcula KDE se houver mais de um ponto
        kde = stats.gaussian_kde(df_negativos['idade'])
        x_range = np.linspace(min_idade, max_idade, 200)
        kde_y = kde(x_range) * len(df_negativos['idade']) * (max_idade - min_idade) / 10
        
        # Adicionar a linha de densidade
        fig.add_trace(go.Scatter(
            x=x_range,
            y=kde_y,
            mode='lines',
            name='Densidade',
            line=dict(color='black', width=2),
            yaxis='y'
        ))
    
    # Personalizar o layout
    fig.update_layout(
        title={
            'text': f'Distribuição de Idades Negativas dos Clientes (Total: {total_negativos})',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title='Idade',
        yaxis_title='Número de Clientes',
        xaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16}
        },
        yaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16},
            'showgrid': False,
            'showline': False
        },
        plot_bgcolor='white',
        height=500,
        width=800,
        showlegend=False,
        margin=dict(t=100, l=70, r=40, b=60),
        bargap=0  # Remove o espaço entre as barras
    )
    
    # Remover linhas de grade e bordas
    fig.update_xaxes(showgrid=False, showline=False)
    
    return fig, total_negativos


def criar_grafico_distribuicao_genero(df_clientes):
   """
   Cria um gráfico de barras interativo mostrando a distribuição de gênero dos clientes.
   
   Args:
       df_clientes (pd.DataFrame): DataFrame contendo a coluna 'genero'
       
   Returns:
       fig: Figura do Plotly pronta para ser exibida
   """
   # Calcular a contagem de gêneros
   contagem = df_clientes['genero'].value_counts().reset_index()
   contagem.columns = ['genero', 'contagem']
   
   # Definir cores diferentes para cada gênero
   cores = {
       'M': '#8B0000',  # Vermelho escuro
       'F': '#FF6B6B',  # Vermelho claro
       'I': '#CD5C5C'   # Vermelho intermediário
   }
   
   # Criar o gráfico
   fig = go.Figure()
   
   # Adicionar as barras
   fig.add_trace(go.Bar(
       x=contagem['genero'],
       y=contagem['contagem'],
       marker=dict(
           color=[cores.get(genero, 'red') for genero in contagem['genero']],
           line=dict(
               color='white',
               width=1
           )
       ),
       text=contagem['contagem'].apply(lambda x: f'{int(x):,}'.replace(',', '.')),
       textposition='outside',
       textfont=dict(
           size=14  # Diminuí o tamanho da fonte dos valores
       )
   ))
   
   # Personalizar o layout
   fig.update_layout(
       title={
           'text': 'Distribuição de Gênero dos Clientes',
           'y': 0.9,  # Reduzi a distância do título
           'x': 0.5,
           'xanchor': 'center',
           'yanchor': 'top',
           'font': {'size': 20}
       },
       xaxis_title='Gênero',
       yaxis_title='Número de Clientes',
       xaxis={
           'tickfont': {'size': 16},  # Aumentei o tamanho da fonte das labels do eixo x
           'title_font': {'size': 16}
       },
       yaxis={
           'showticklabels': False,  # Remove os números do eixo y
           'showgrid': False,
           'showline': False,
           'range': [0, max(contagem['contagem']) * 1.05]  # Reduzi um pouco o espaço superior
       },
       plot_bgcolor='white',
       height=500,
       width=800,
       showlegend=False,
       margin=dict(t=100, l=70, r=40, b=60)  # Reduzi a margem superior
   )
   
   # Remover linhas de grade e bordas
   fig.update_xaxes(showgrid=False, showline=False)
   fig.update_yaxes(showgrid=False, showline=False)
   
   return fig


def criar_grafico_distribuicao_compras(df_clientes):
    """
    Cria um gráfico de barras interativo mostrando a distribuição das últimas compras por data.
    
    Args:
        df_clientes (pd.DataFrame): DataFrame contendo a coluna 'data_ultima_compra_renner'
        
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    
    # Converter datas para datetime se ainda não estiver nesse formato
    df_clientes['data_ultima_compra_renner'] = pd.to_datetime(df_clientes['data_ultima_compra_renner'])
    
    # Contar o número de compras por data
    contagem_diaria = df_clientes['data_ultima_compra_renner'].value_counts().sort_index()
    
    # Criar o gráfico
    fig = go.Figure()
    
    # Adicionar as barras (uma por data)
    fig.add_trace(go.Bar(
        x=contagem_diaria.index,
        y=contagem_diaria.values,
        name='Compras',
        marker=dict(
            color='red',
            line=dict(
                color='white',
                width=0.1
            )
        )
    ))
    
    # Aplicar o filtro Savitzky-Golay para suavizar os dados
    window_length = 51  # Deve ser ímpar
    polyorder = 3
    yhat = savgol_filter(contagem_diaria.values, window_length, polyorder)
    
    # Adicionar a linha suavizada
    fig.add_trace(go.Scatter(
        x=contagem_diaria.index,
        y=yhat,
        mode='lines',
        name='Tendência',
        line=dict(color='black', width=2),
        yaxis='y'
    ))
    
    # Personalizar o layout
    fig.update_layout(
        title={
            'text': 'Distribuição das Últimas Compras por Data',
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title='Data da Última Compra',
        yaxis_title='Número de Compras',
        xaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16}
        },
        yaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16},
            'showgrid': True,
            'gridwidth': 0.3,
            'gridcolor': 'lightgray',
            'showline': False,
            'dtick': 100,
            'range': [0, max(contagem_diaria.values) * 1.1]
        },
        plot_bgcolor='white',
        height=500,
        width=1000,
        showlegend=False,
        margin=dict(t=80, l=70, r=40, b=60),
        bargap=0
    )
    
    # Remover linhas de grade e bordas
    fig.update_xaxes(showgrid=False, showline=False)
    
    return fig


def criar_grafico_intervalo_compras(df_clientes):
    """
    Cria um histograma interativo mostrando a distribuição do intervalo entre primeira e última compra.
    
    Args:
        df_clientes (pd.DataFrame): DataFrame contendo as colunas de datas de compra
        
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    df_clientes['data_ultima_compra_renner'] = pd.to_datetime(df_clientes['data_ultima_compra_renner'])
    df_clientes['data_primeira_compra_renner'] = pd.to_datetime(df_clientes['data_primeira_compra_renner'])

    # Calcular o intervalo
    df_clientes['intervalo_pri_ult_compra'] = pd.to_timedelta(
        df_clientes['data_ultima_compra_renner'] - df_clientes['data_primeira_compra_renner']
    ).dt.days

    # Criar o gráfico
    fig = go.Figure()
    
    # Criar o histograma
    fig.add_trace(go.Histogram(
        x=df_clientes['intervalo_pri_ult_compra'],
        nbinsx=200,  # Mesmo número de bins do seaborn default
        name='Histograma',
        marker=dict(
            color='red',
            line=dict(
                color='white',
                width=1
            )
        )
    ))
    
    # Calcular KDE usando o mesmo método do seaborn
    data = df_clientes['intervalo_pri_ult_compra'].dropna()
    
    # Usar os parâmetros padrão do seaborn
    kde = gaussian_kde(data, bw_method='scott')  # Scott's rule - mesmo do seaborn
    x_range = np.linspace(data.min(), data.max(), 200)
    y_kde = kde(x_range)
    
    # Ajustar a escala do KDE para corresponder ao histograma
    hist, bin_edges = np.histogram(data, bins=200, density=True)
    scaling_factor = len(data) * (bin_edges[1] - bin_edges[0])
    y_kde = y_kde * scaling_factor
    
    # Adicionar linha KDE
    fig.add_trace(go.Scatter(
        x=x_range,
        y=y_kde,
        mode='lines',
        name='KDE',
        line=dict(
            color='black',
            width=2
        )
    ))
    
    # Personalizar o layout
    fig.update_layout(
        title={
            'text': 'Intervalo entre a Primeira e a Última Compra',
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title='Número de Dias',
        yaxis_title='Número de Clientes',
        xaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16}
        },
        yaxis={
            'tickfont': {'size': 14},
            'title_font': {'size': 16},
            'showgrid': True,
            'gridwidth': 0.3,
            'gridcolor': 'lightgray'
        },
        plot_bgcolor='white',
        showlegend=False,
        bargap=0
    )
    
    # Ajustar os limites dos eixos para match com original
    fig.update_xaxes(range=[-1000, 4500], showgrid=False)
    fig.update_yaxes(range=[0, 3900])
    
    return fig


def transformacoes_grafico_cidades(df_clientes: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """
    Faz as transformações necessárias para criar o gráfico de cidades.

    :param df_clientes: DataFrame com os dados dos clientes
    :return: DataFrame contendo as cidades que representam até 35% do total de clientes
    """
    # Calculando a quantidade de cidades que representa 35% do total de clientes
    cidade_counts = df_clientes['cidade'].value_counts()
    total_clientes = cidade_counts.sum()

    # Preparando os dados para o gráfico de percentuais acumulados das cidades
    cidade_counts_sorted = cidade_counts.sort_values(ascending=False)
    cidade_counts_cumsum_sorted = cidade_counts_sorted.cumsum() / total_clientes

    # Filtrando as cidades que representam até 35% do total de clientes
    cidades_35_percent_data = cidade_counts_sorted[cidade_counts_cumsum_sorted <= 0.35]
    percentual_acumulado_35 = cidade_counts_cumsum_sorted[cidade_counts_cumsum_sorted <= 0.35]

    return cidades_35_percent_data, percentual_acumulado_35


def criar_grafico_cidades_35_percent(cidades_35_percent_data, percentual_acumulado_35):
    """
    Cria um gráfico de barras com linha de percentual acumulado mostrando as cidades 
    que representam até 35% do total de clientes.
    """
    # Criar paleta de cores vermelhas
    colors = ['#67001f', '#b2182b', '#d6604d', '#f4a582', '#fddbc7', '#fee3d6', '#fee8e7', '#fff0ed']
    
    # Criar o gráfico
    fig = go.Figure()

    # Adicionar as barras
    fig.add_trace(go.Bar(
        x=cidades_35_percent_data.index,
        y=cidades_35_percent_data.values,
        name='Número de Clientes',
        marker=dict(
            color=colors,
            line=dict(
                color='white',
                width=1
            )
        )
    ))

    # Adicionar a linha de percentual acumulado
    fig.add_trace(go.Scatter(
        x=percentual_acumulado_35.index,
        y=percentual_acumulado_35.values * 100,
        name='Percentual Acumulado',
        mode='lines+markers',
        line=dict(
            color='red',
            width=2
        ),
        marker=dict(
            color='red',
            size=8
        ),
        yaxis='y2'
    ))

    # Personalizar o layout
    fig.update_layout(
        title={
            'text': 'Cidades Representando Até 35% do Total de Clientes',
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title={
            'text': 'Cidade',
            'font': {'size': 12}
        },
        yaxis_title='Número de Clientes',
        yaxis2=dict(
            title={
                'text': 'Percentual Acumulado (%)',
                'font': {'size': 14}
            },
            overlaying='y',
            side='right',
            showgrid=False,
            zeroline=False,
            tickfont={'size': 12}
        ),
        xaxis={
            'tickfont': {'size': 11},
            'tickangle': 0
        },
        yaxis={
            'tickfont': {'size': 11},
            'title_font': {'size': 14},
            'showgrid': True,
            'gridwidth': 0.3,
            'gridcolor': 'lightgray',
            'dtick': 500
        },
        plot_bgcolor='white',
        height=500,
        width=1000,
        showlegend=False,
        bargap=0.2
    )

    # Remover linhas de grade do eixo x
    fig.update_xaxes(showgrid=False)

    return fig


def criar_grafico_eventos_jornada(df_navegacao):
   """
   Cria um gráfico de barras mostrando a contagem de eventos por tipo na jornada de compra.
   
   Args:
       df_navegacao: DataFrame contendo as colunas 'data_evento' e 'nome_evento'
       
   Returns:
       fig: Figura do Plotly pronta para ser exibida
   """
   # Converter a data para datetime
   df_navegacao['data_evento'] = pd.to_datetime(df_navegacao['data_evento'])
   
   # Ordem lógica dos eventos
   ordem_eventos = ['view_item', 'select_item', 'add_to_wishlist', 'add_to_cart', 'purchase']
   
   # Calcular contagem de eventos
   contagem_eventos = df_navegacao['nome_evento'].value_counts()
   # Reordenar conforme a ordem lógica
   contagem_eventos = contagem_eventos.reindex(ordem_eventos)
   
   # Criar o gráfico
   fig = go.Figure()
   
   # Criar paleta de cores vermelhas (do mais escuro ao mais claro)
   colors = ['#67001f', '#b2182b', '#d6604d', '#f4a582', '#fddbc7']
   
   # Adicionar as barras
   fig.add_trace(go.Bar(
       x=contagem_eventos.index,
       y=contagem_eventos.values,
       marker=dict(
           color=colors,
           line=dict(
               color='white',
               width=1
           )
       ),
       text=contagem_eventos.values.round(0).astype(int),  # Valores inteiros
       textposition='outside',
       textfont=dict(
           size=12
       )
   ))
   
   # Personalizar o layout
   fig.update_layout(
       title={
           'text': 'Contagem de Eventos por Tipo na Jornada de Compra',
           'y': 0.9,
           'x': 0.5,
           'xanchor': 'center',
           'yanchor': 'top',
           'font': {'size': 20}
       },
       xaxis_title={
           'text': 'Tipo de Evento',
           'font': {'size': 14}
       },
       yaxis_title={
           'text': 'Número de Eventos',
           'font': {'size': 14}
       },
       xaxis={
           'tickfont': {'size': 12},
           'tickangle': 0
       },
       yaxis={
           'tickfont': {'size': 12},
           'showgrid': True,
           'gridwidth': 0.3,
           'gridcolor': 'lightgray',
           'dtick': 10000,
           'tickformat': 'd',  # Formato para mostrar números inteiros sem separador de milhar
           'separatethousands': True  # Adiciona separador de milhar
       },
       plot_bgcolor='white',
       height=500,
       width=800,
       showlegend=False,
       bargap=0.2
   )
   
   # Remover linhas de grade do eixo x
   fig.update_xaxes(showgrid=False)
   
   return fig


def criar_grafico_tipo_venda(df_transacao):
   """
   Cria um gráfico de barras mostrando a distribuição dos tipos de venda.
   
   Args:
       df_transacao: DataFrame contendo a coluna 'tipo_venda'
       
   Returns:
       fig: Figura do Plotly pronta para ser exibida
   """
   # Calcular contagem de tipos de venda e ordenar
   contagem_vendas = df_transacao['tipo_venda'].value_counts().reindex(['ON', 'OFF'])
   
   # Criar o gráfico
   fig = go.Figure()
   
   # Criar paleta de cores vermelhas (ON mais escuro, OFF mais claro)
   colors = ['#b2182b', '#fddbc7']  # Vermelho escuro para ON, claro para OFF
   
   # Encontrar o valor máximo para ajustar o range do eixo y
   valor_maximo = contagem_vendas.max()
   
   # Adicionar as barras
   fig.add_trace(go.Bar(
       x=contagem_vendas.index,
       y=contagem_vendas.values,
       marker=dict(
           color=colors,
           line=dict(
               color='white',
               width=1
           )
       ),
       text=contagem_vendas.values.round(0).astype(int),
       textposition='outside',
       textfont=dict(
           size=16
       )
   ))
   
   # Personalizar o layout
   fig.update_layout(
       title={
           'text': 'Distribuição do Tipo de Venda',
           'y': 0.9,
           'x': 0.5,
           'xanchor': 'center',
           'yanchor': 'top',
           'font': {'size': 20}
       },
       xaxis_title={
           'text': 'Tipo de Venda',
           'font': {'size': 14}
       },
       yaxis_title={
           'text': 'Contagem de Vendas',
           'font': {'size': 14}
       },
       xaxis={
           'tickfont': {'size': 12},
           'tickangle': 0
       },
       yaxis={
           'tickfont': {'size': 12},
           'showgrid': True,
           'gridwidth': 0.3,
           'gridcolor': 'lightgray',
           'dtick': 100000,
           'tickformat': 'd',
           'separatethousands': True,
           'range': [0, valor_maximo * 1.1]  # Aumentar o range em 10% para caber o texto
       },
       plot_bgcolor='white',
       height=500,
       width=800,
       showlegend=False,
       bargap=0.2
   )
   
   # Remover linhas de grade do eixo x
   fig.update_xaxes(showgrid=False)
   
   return fig


def criar_grafico_boxplot_divisao(df_transacao):
    """
    Cria um boxplot mostrando a distribuição dos valores por divisão.
    
    Args:
        df_transacao: DataFrame contendo as colunas 'nome_divisao' e 'valor'
        
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    # Criar o gráfico
    fig = go.Figure()
    
    # Adicionar os boxplots para cada divisão
    for divisao in df_transacao['nome_divisao'].unique():
        fig.add_trace(go.Box(
            y=df_transacao[df_transacao['nome_divisao'] == divisao]['valor'],
            name=divisao,
            fillcolor='red',  # cor do preenchimento
            boxpoints='outliers',  # mostrar outliers
            line=dict(color='black', width=1),  # cor da borda do boxplot
            marker=dict(
                color='black',  # cor dos outliers
                size=3,  # tamanho menor dos outliers
                outliercolor='black'  # cor dos outliers
            ),
            hoverinfo='skip',  # desabilita hover nos outliers
            hoveron='boxes'  # hover apenas nas caixas
        ))
    
    # Personalizar o layout
    fig.update_layout(
        title={
            'text': 'Valor por Divisão',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 20}
        },
        xaxis_title={
            'text': 'Divisão',
            'font': {'size': 12}
        },
        yaxis_title={
            'text': 'Valor',
            'font': {'size': 12}
        },
        xaxis={
            'tickfont': {'size': 8},
            'tickangle': 0
        },
        yaxis={
            'type': 'log',
            'tickfont': {'size': 10},
            'showgrid': True,
            'gridwidth': 0.3,
            'gridcolor': 'lightgray',
            'tickformat': '.0e',
            'tickmode': 'array',
            'tickvals': [1, 10, 100, 1000, 10000]
        },
        plot_bgcolor='white',
        showlegend=False,
        boxgap=0.3,
        margin=dict(r=20, l=50, b=50, t=100, pad=4)
    )
    
    # Remover linhas de grade do eixo x
    fig.update_xaxes(showgrid=False)
    
    return fig


def plot_sales_value_distribution(df_transacao: pd.DataFrame) -> go.Figure:
    """
    Cria um gráfico de distribuição dos valores de venda usando Plotly, 
    incluindo o gráfico de densidade (KDE).

    Args:
        df_transacao (pd.DataFrame): DataFrame contendo os dados
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    fig = go.Figure()

    # Criar o histograma
    fig.add_trace(go.Histogram(
        x=df_transacao['valor'],
        nbinsx=10,
        marker_color='red',
        opacity=0.7,
        showlegend=False
    ))

    # Calcular e adicionar o KDE
    kde_x = np.linspace(df_transacao['valor'].min(), df_transacao['valor'].max(), 100)
    kde_y = stats.gaussian_kde(df_transacao['valor'])(kde_x)
    
    # Escalar o KDE para corresponder à escala do histograma
    bin_width = (df_transacao['valor'].max() - df_transacao['valor'].min()) / 10
    scaling_factor = len(df_transacao['valor']) * bin_width

    fig.add_trace(go.Scatter(
        x=kde_x,
        y=kde_y * scaling_factor,
        mode='lines',
        line=dict(color='black', width=2),
        showlegend=False
    ))

    # Configurar o layout
    fig.update_layout(
        title={
            'text': 'Distribuição dos Valores de Venda',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 16}
        },
        xaxis_title='Valor (R$)',
        yaxis_title='Frequência',
        bargap=0.1,
        plot_bgcolor='white',
        width=800,
        height=500,
        showlegend=False,
        margin=dict(l=50, r=20, t=50, b=50)  # Reduzindo margem direita
    )

    # Definir ticks do eixo y em escala 1e6
    y_ticks = np.array([0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5])
    fig.update_layout(
        yaxis=dict(
            title='Frequência',
            tickmode='array',
            tickvals=y_ticks * 1e6,
            ticktext=[f'{val:.1f}' for val in y_ticks],
            tickformat='.1f',
            range=[0, 3.5e6],  # Fixando o range do eixo y
            showgrid=False  # Removendo grid do eixo y
        ),
        xaxis=dict(
            range=[-200, 8000],  # Começando antes do 0 para dar espaço
            dtick=1000,  # Definindo o intervalo entre os ticks
            showgrid=False  # Removendo grid do eixo x
        )
    )

    # Adicionar notação 1e6 no canto superior esquerdo
    fig.add_annotation(
        text="1e6",
        xref="paper",
        yref="paper",
        x=0,
        y=1,
        xanchor="right",
        yanchor="bottom",
        showarrow=False,
        font=dict(size=12)
    )

    return fig


def transformacao_grafico_vendas_item(df_transacao: pd.DataFrame) -> pd.DataFrame:
    """
    Faz as transformações necessárias para criar o gráfico de vendas por item

    :param df_transacao: DataFrame com os dados das transações
    :return: DataFrame contendo os itens com seus valores de venda
    """
    # Criar uma nova coluna com a quantidade de vendas por item
    df_vendas_item = df_transacao.groupby('codigo_item').agg({'valor': ['sum', 'count']}).reset_index()
    df_vendas_item.columns = ['codigo_item', 'valor_total', 'qtd_vendas']
    df_vendas_item['codigo_item'] = df_vendas_item['codigo_item'].astype(str)

    # Agrupar itens por faixas de volume de vendas
    df_vendas_item['faixa_vendas'] = pd.cut(df_vendas_item['qtd_vendas'], bins=[1, 10, 100, 1000, 10000], labels=['1-10', '11-100', '101-1000', '1001-10000'])

    return df_vendas_item


def plot_top_items_sales(df_vendas_item, top_n=10):
    """
    Cria um gráfico de barras horizontal com os itens mais vendidos usando Plotly.
    
    Args:
        df_vendas_item (pd.DataFrame): DataFrame contendo as colunas 'codigo_item' e 'valor_total'
        top_n (int): Número de itens top a serem mostrados (default: 10)
    
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    # Identificar os top N itens em ordem decrescente de valor
    df_top_items = df_vendas_item.nlargest(top_n, 'valor_total')
    
    # Converter código do item para string para exibição
    df_top_items = df_top_items.copy()
    df_top_items['codigo_item'] = df_top_items['codigo_item'].astype(str)
    
    # Criar escala de cores personalizada do mais claro para o mais escuro
    colors = np.linspace(0, 1, top_n)
    custom_colors = [f'rgb({int(255-(155*x))}, {int(80-(60*x))}, {int(80-(60*x))})' for x in colors]
    
    # Criar figura
    fig = go.Figure()
    
    # Adicionar barras horizontais
    fig.add_trace(go.Bar(
        x=df_top_items['valor_total'].values,
        y=df_top_items['codigo_item'].values,
        orientation='h',
        marker=dict(
            color=custom_colors,
        ),
        text=[f'R$ {valor:,.2f}' for valor in df_top_items['valor_total'].values],
        textposition='outside',
        textfont=dict(size=10),
        hoverinfo='none'
    ))
    
    # Atualizar layout
    fig.update_layout(
        title={
            'text': f'Top {top_n} Itens Mais Vendidos em R$',
            'y': 0.98,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        xaxis_title='Valor Total de Vendas (R$)',
        yaxis_title='Código do Item',
        plot_bgcolor='white',
        width=900,
        height=600,
        showlegend=False,
        margin=dict(l=50, r=80, t=50, b=50),
        yaxis=dict(
            tickfont=dict(size=11),
            autorange="reversed"
        ),
        xaxis=dict(
            range=[0, 3_000_000],  # Fixado em 3 milhões
            tickformat=',',
            tickfont=dict(size=10),
            showgrid=True,
            gridwidth=1,
            gridcolor='rgba(128, 128, 128, 0.2)',
            dtick=500000  # Definindo o intervalo dos ticks para cada 500 mil
        )
    )
    
    # Remover grid do eixo y
    fig.update_yaxes(
        showgrid=False,
        zeroline=False
    )
    
    return fig


def plot_item_boxplot(df_transacao, codigo_item):
    """
    Cria um boxplot para um item específico usando Plotly.
    
    Args:
        df_transacao (pd.DataFrame): DataFrame contendo as colunas 'codigo_item' e 'valor'
        codigo_item (int): Código do item para análise
    
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    # Filtrar dados para o item específico
    df_item = df_transacao.loc[df_transacao['codigo_item'] == codigo_item]
    
    # Criar figura
    fig = go.Figure()
    
    # Adicionar boxplot
    fig.add_trace(go.Box(
        y=df_item['valor'],
        name=f'Item {codigo_item}',
        boxpoints='outliers',  # Mostrar apenas outliers
        marker=dict(
            color='black',  # Cor dos outliers
            size=4,  # Tamanho dos outliers
            opacity=0.7
        ),
        line=dict(
            color='black',  # Cor das linhas do boxplot
            width=1  # Espessura das linhas
        ),
        fillcolor='red',  # Cor de preenchimento da caixa
        showlegend=False
    ))
    
    # Atualizar layout
    fig.update_layout(
        title={
            'text': f'Valor de Venda do Item {codigo_item}',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        yaxis_title='valor',
        xaxis_title=f'Item {codigo_item}',
        plot_bgcolor='white',
        width=800,
        height=600,
        showlegend=False,
        yaxis=dict(
            range=[0, 4500],  # Ajustando o range do eixo y
            showgrid=False,  # Removendo grid
            zeroline=False,
            dtick=1000  # Intervalo dos ticks
        ),
        xaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False  # Removendo linha do eixo x
        )
    )
    
    # Remover linha direita do frame e manter apenas a linha esquerda
    fig.update_layout(
        yaxis=dict(
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False
        )
    )
    
    return fig


def transformacoes_grafico_variacao(df_transacao: pd.DataFrame) -> pd.DataFrame:
    """
    Faz as transformações necessárias para criar o gráfico de variação de vendas.

    :param df_transacao: DataFrame com os dados das transações
    :return: DataFrame contendo a variação de vendas por mês
    """
    # Definir a função para calcular a moda
    def calcular_moda(serie):
        # Usar pandas mode() que retorna uma Series, pegar o primeiro valor se existir
        moda = serie.mode()
        return moda[0] if len(moda) > 0 else np.nan

    # Calcular a variação de preço para cada item, quantidade vendida e moda do preço
    df_variacao = df_transacao.groupby('codigo_item').agg(
        preco_medio=('valor', 'mean'),
        preco_min=('valor', 'min'),
        preco_max=('valor', 'max'),
        desvio_padrao=('valor', 'std'),
        qtd_vendida=('valor', 'count'),  # Contagem de vendas (quantidade vendida)
        moda_preco=('valor', calcular_moda)  # Usar a função de moda personalizada
    ).reset_index()

    # Calcular a amplitude (range) e o coeficiente de variação (CV)
    df_variacao['amplitude'] = df_variacao['preco_max'] - df_variacao['preco_min']
    df_variacao['cv'] = (df_variacao['desvio_padrao'] / df_variacao['preco_medio']) * 100  # Coeficiente de Variação

    df_variacao['cv'] = df_variacao['cv'].fillna(0)
    df_variacao['desvio_padrao'] = df_variacao['desvio_padrao'].fillna(0)

    # Ordenar pelos itens com maior variação de preço (pelo desvio padrão ou amplitude)
    df_variacao = df_variacao.sort_values(by='amplitude', ascending=False)

    # Mostrar os itens com maior variação
    df_variacao.sort_values('cv', ascending=False)

    return df_variacao


def plot_cv_distribution(df_variacao):
    """
    Cria um histograma da distribuição do coeficiente de variação usando Plotly.
    
    Args:
        df_variacao (pd.DataFrame): DataFrame contendo a coluna 'cv'
    
    Returns:
        fig: Figura do Plotly pronta para ser exibida
    """
    # Criar figura
    fig = go.Figure()
    
    # Adicionar histograma
    fig.add_trace(go.Histogram(
        x=df_variacao['cv'],
        nbinsx=400,  # Número de bins para melhor granularidade
        marker_color='red',
        opacity=1,
        hoverinfo='skip'
    ))
    
    # Atualizar layout
    fig.update_layout(
        title={
            'text': 'Distribuição do Coeficiente de Variação',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        xaxis_title='Coeficiente de Variação',
        yaxis_title='Contagem',
        plot_bgcolor='white',
        width=800,
        height=500,
        showlegend=False,
        xaxis=dict(
            range=[0, 500],  # Range do eixo x
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False
        ),
        yaxis=dict(
            type='log',  # Escala logarítmica
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False,
            range=[0, 5],  # Range do log de 10^0 até 10^5
        )
    )
    
    # Ajustar margens
    fig.update_layout(
        margin=dict(l=50, r=20, t=50, b=50)
    )
    
    return fig


def plot_age_distribution_etl(df_clientes: pd.DataFrame) -> go.Figure:
    """
    Creates an interactive histogram with KDE showing the age distribution of clients.

    :param df_clientes: DataFrame containing the age column
    :return fig: Plotly figure ready to be displayed
    """
    df_clientes = df_clientes.loc[df_clientes['idade'] > 0]

    bin_edges = np.linspace(df_clientes['idade'].min(), df_clientes['idade'].max(), 11)

    fig = go.Figure()

    # Add histogram trace
    fig.add_trace(go.Histogram(
        x=df_clientes['idade'],
        xbins=dict(
            start=bin_edges[0],
            end=bin_edges[-1],
            size=(bin_edges[-1] - bin_edges[0]) / 10
        ),
        name='Histogram',
        marker=dict(
            color='rgba(255, 0, 0, 0.5)',
            line=dict(color='black', width=1)
        )
    ))

    # Calculate and add KDE curve
    kde_x = np.linspace(df_clientes['idade'].min(), df_clientes['idade'].max(), 100)
    kde = stats.gaussian_kde(df_clientes['idade'])
    kde_y = kde(kde_x) * len(df_clientes['idade']) * (df_clientes['idade'].max() - df_clientes['idade'].min()) / 10

    fig.add_trace(go.Scatter(
        x=kde_x,
        y=kde_y,
        mode='lines',
        name='KDE',
        line=dict(color='red', width=2)
    ))

    # Update layout
    fig.update_layout(
        title='Distribuição de Idade dos Clientes',
        title_font_size=14,
        xaxis_title='Idade',
        yaxis_title='Número de Clientes',
        showlegend=False,
        plot_bgcolor='white',
    )

    return fig


def plot_age_distribution_over_16(df_clientes: pd.DataFrame) -> go.Figure:
    """
    Creates an interactive histogram with KDE showing the age distribution of clients.

    :param df_clientes: DataFrame containing the age column
    :return fig: Plotly figure ready to be displayed
    """
    df_clientes = df_clientes.loc[df_clientes['idade'] >= 16]

    # Calculate bin edges manually for exactly 10 bins
    bin_edges = np.linspace(df_clientes['idade'].min(), df_clientes['idade'].max(), 11)
    
    fig = go.Figure()

    # Add histogram trace with fixed bins and black border
    fig.add_trace(go.Histogram(
        x=df_clientes['idade'],
        xbins=dict(
            start=bin_edges[0],
            end=bin_edges[-1],
            size=(bin_edges[-1] - bin_edges[0]) / 10
        ),
        name='Histogram',
        marker=dict(
            color='rgba(255, 0, 0, 0.5)',
            line=dict(color='black', width=1)
        )
    ))

    # Calculate and add KDE curve
    kde_x = np.linspace(df_clientes['idade'].min(), df_clientes['idade'].max(), 100)
    kde = stats.gaussian_kde(df_clientes['idade'])
    kde_y = kde(kde_x) * len(df_clientes['idade']) * (df_clientes['idade'].max() - df_clientes['idade'].min()) / 10

    fig.add_trace(go.Scatter(
        x=kde_x,
        y=kde_y,
        mode='lines',
        name='KDE',
        line=dict(color='red', width=2)
    ))

    # Update layout
    fig.update_layout(
        title='Distribuição de Idade dos Clientes',
        title_font_size=14,
        xaxis_title='Idade',
        yaxis_title='Número de Clientes',
        showlegend=False,
        plot_bgcolor='white',
    )

    return fig


def plot_purchase_interval(df_clientes: pd.DataFrame) -> go.Figure:
    """
    Creates an interactive histogram with KDE showing the distribution of time intervals
    between first and last purchases.

    :param df_clientes: DataFrame containing purchase date columns
    :return fig: Plotly figure ready to be displayed
    """
    # Fix reversed dates
    condicao = df_clientes['data_ultima_compra_renner'] < df_clientes['data_primeira_compra_renner']
    df_clientes.loc[condicao, ['data_ultima_compra_renner', 'data_primeira_compra_renner']] = \
        df_clientes.loc[condicao, ['data_primeira_compra_renner', 'data_ultima_compra_renner']].values

    # Calculate interval
    df_clientes['intervalo_pri_ult_compra'] = pd.to_timedelta(
        df_clientes['data_ultima_compra_renner'] - df_clientes['data_primeira_compra_renner']
    ).dt.days

    data = df_clientes['intervalo_pri_ult_compra'].dropna()
    
    fig = go.Figure()

    # Add histogram trace
    fig.add_trace(go.Histogram(
        x=data,
        nbinsx=100,  # Increased number of bins to match image
        name='Histogram',
        marker=dict(
            color='rgba(255, 0, 0, 0.5)',
            line=dict(color='black', width=0.5)
        )
    ))

    # Calculate and add KDE curve
    kde_x = np.linspace(data.min(), data.max(), 200)  # Increased points for smoother curve
    kde = stats.gaussian_kde(data)
    kde_y = kde(kde_x) * len(data) * (data.max() - data.min()) / 100

    fig.add_trace(go.Scatter(
        x=kde_x,
        y=kde_y,
        mode='lines',
        name='KDE',
        line=dict(color='black', width=2)
    ))

    # Update layout
    fig.update_layout(
        title='Intervalo entre a Primeira e a Última Compra',
        title_font_size=14,
        xaxis_title='Número de Dias',
        yaxis_title='Número de Clientes',
        showlegend=False,
        plot_bgcolor='white',
        xaxis=dict(range=[-200, 4000]),  # Set x-axis range to match image
        yaxis=dict(range=[0, 4500]),  # Set y-axis range to match image
    )

    return fig


def transformacoes_etl_heuristicas(df_transacao: pd.DataFrame) -> pd.DataFrame:
    """
    Faz as transformações necessárias para criar o gráfico de distribuição de valores de venda.

    :param df_transacao: DataFrame com os dados das transações
    :return: DataFrame contendo os valores de venda
    """
    df_transacao = df_transacao.loc[df_transacao['codigo_item']!= 108799]

    # Definir a função para calcular a moda
    def calcular_moda(serie):
        # Usar pandas mode() que retorna uma Series, pegar o primeiro valor se existir
        moda = serie.mode()
        return moda[0] if len(moda) > 0 else np.nan

    # Calcular a variação de preço para cada item, quantidade vendida e moda do preço
    df_variacao = df_transacao.groupby('codigo_item').agg(
        preco_medio=('valor', 'mean'),
        preco_min=('valor', 'min'),
        preco_max=('valor', 'max'),
        desvio_padrao=('valor', 'std'),
        qtd_vendida=('valor', 'count'),  # Contagem de vendas (quantidade vendida)
        moda_preco=('valor', calcular_moda)  # Usar a função de moda personalizada
    ).reset_index()

    # Calcular a amplitude (range) e o coeficiente de variação (CV)
    df_variacao['amplitude'] = df_variacao['preco_max'] - df_variacao['preco_min']
    df_variacao['cv'] = (df_variacao['desvio_padrao'] / df_variacao['preco_medio']) * 100

    df_variacao['cv'] = df_variacao['cv'].fillna(0)
    df_variacao['desvio_padrao'] = df_variacao['desvio_padrao'].fillna(0)

    df_variacao = df_variacao.sort_values(by='amplitude', ascending=False)

    return df_variacao


def plot_variation_coefficient(df_variacao: pd.DataFrame) -> tuple[go.Figure, pd.DataFrame]:
    """
    Creates an interactive histogram showing the distribution of variation coefficients.
    Uses log scale for y-axis and filters for prices >= 1.

    :param df_variacao: DataFrame containing 'moda_preco' and 'cv' columns
    :return fig: Plotly figure ready to be displayed
    :return df_variacao_m1: Filtered DataFrame with prices >= 1
    """
    # Filter for prices >= 1
    df_variacao_m1 = df_variacao.loc[df_variacao['moda_preco'] >= 1]

    # Create figure
    fig = go.Figure()

    # Add histogram trace
    fig.add_trace(go.Histogram(
        x=df_variacao_m1['cv'],
        nbinsx=400,
        marker=dict(
            color='red',
            line=dict(color='black', width=0.5)
        ),
        opacity=1,
        hoverinfo='skip'
    ))

    # Update layout
    fig.update_layout(
        title={
            'text': 'Distribuição do Coeficiente de Variação',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        xaxis_title='Coeficiente de Variação',
        yaxis_title='Contagem',
        plot_bgcolor='white',
        width=800,
        height=500,
        showlegend=False,
        xaxis=dict(
            range=[-20, 500],
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False
        ),
        yaxis=dict(
            type='log',
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False,
            range=[-0.1, 5]  # Changed to start at 10^-2 and go up to 10^5
        ),
        margin=dict(l=50, r=20, t=50, b=50)
    )

    return fig, df_variacao_m1


def plot_filtered_variation_coefficient(df_variacao_m1: pd.DataFrame) -> tuple[go.Figure, pd.DataFrame]:
    """
    Creates an interactive histogram showing the distribution of variation coefficients
    for items with standard deviation <= 3.

    :param df_variacao_m1: DataFrame containing 'desvio_padrao' and 'cv' columns
    :return fig: Plotly figure ready to be displayed
    :return df_itens: Filtered DataFrame with standard deviation <= 3
    """
    # Filter for standard deviation <= 3
    df_itens = df_variacao_m1.loc[df_variacao_m1['desvio_padrao'] <= 3]

    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=df_itens['cv'],
        nbinsx=21,
        marker=dict(
            color='red',
            line=dict(color='black', width=0.5)
        ),
        opacity=1,
        hoverinfo='skip'
    ))

    fig.update_layout(
        title={
            'text': 'Distribuição do Coeficiente de Variação',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        xaxis_title='Coeficiente de Variação',
        yaxis_title='Contagem',
        plot_bgcolor='white',
        showlegend=False,
        xaxis=dict(
            range=[-2, 38],  # Updated x-axis range
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False
        ),
        yaxis=dict(
            type='log',
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False,
            range=[-0.1, 5]
        ),
        margin=dict(l=50, r=20, t=50, b=50)
    )

    return fig, df_itens


def plot_filtered_variation_coefficient_restrictive(df_variacao_m1: pd.DataFrame) -> go.Figure:
    """
    Creates an interactive histogram showing the distribution of variation coefficients
    for items with standard deviation < 1.5.

    :param df_variacao_m1: DataFrame containing 'desvio_padrao' and 'cv' columns
    :return fig: Plotly figure ready to be displayed
    """
    df_itens = df_variacao_m1.loc[df_variacao_m1['desvio_padrao'] < 1.5]

    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=df_itens['cv'],
        nbinsx=21,
        marker=dict(
            color='red',
            line=dict(color='black', width=0.5)
        ),
        opacity=1,
        hoverinfo='skip'
    ))

    fig.update_layout(
        title={
            'text': 'Distribuição do Coeficiente de Variação',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        xaxis_title='Coeficiente de Variação',
        yaxis_title='Contagem',
        plot_bgcolor='white',
        width=800,
        height=500,
        showlegend=False,
        xaxis=dict(
            range=[-1, 23],
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False
        ),
        yaxis=dict(
            type='log',
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            showline=True,
            linewidth=1,
            linecolor='black',
            mirror=False,
            range=[-0.1, 5]
        ),
        margin=dict(l=50, r=20, t=50, b=50)
    )

    return fig


def read_csv_files_fe() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lê os arquivos CSV específicos da pasta input do bucket
    e retorna dois dataframes: navegacao e transacao.

    :return df_navegacao: Dataframe com os dados de navegação
    :return df_transacao: Dataframe com os dados de transações
    """
    # Configurações do bucket
    bucket_name = 'bkt-dev-projcdia-rennerrethink-streamlit'
    input_prefix = 'input/'

    # Obtém o cliente S3
    s3_client = get_s3_client()

    # Inicializa os dataframes
    df_navegacao = pd.DataFrame()
    df_transacao = pd.DataFrame()

    try:
        # Lista todos os objetos na pasta input
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=input_prefix
        )

        # Verifica se existem objetos
        if 'Contents' not in response:
            print(f"Nenhum arquivo encontrado em {input_prefix}")
            return df_navegacao, df_transacao

        # Para cada arquivo na pasta input
        for obj in response['Contents']:
            file_key = obj['Key']

            # Verifica se é um arquivo CSV
            if file_key.endswith('.csv'):
                try:
                    # Lê o arquivo CSV
                    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                    file_name = file_key.split('/')[-1].lower()

                    if 'navegacao' in file_name:
                        df_navegacao = pd.read_csv(io.BytesIO(response['Body'].read()))
                        print("Arquivo de navegação lido com sucesso!")
                    elif 'transacao' in file_name:
                        df_transacao = pd.read_csv(io.BytesIO(response['Body'].read()))
                        print("Arquivo de transações lido com sucesso!")

                except Exception as e:
                    print(f"Erro ao ler arquivo {file_key}: {str(e)}")
                    continue

        return df_navegacao, df_transacao

    except Exception as e:
        print(f"Erro ao listar objetos do bucket: {str(e)}")
        raise


def read_parquet_files_fe() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Lê os arquivos parquet específicos da pasta input do bucket
    e retorna dois dataframes: clientes e métricas dos itens.

    :return df_clientes: Dataframe com os dados dos clientes
    :return df_itens_metricas: Dataframe com os dados de métricas dos itens
    """
    # Configurações do bucket
    bucket_name = 'bkt-dev-projcdia-rennerrethink-streamlit'
    input_prefix = 'output/'

    # Obtém o cliente S3
    s3_client = get_s3_client()

    # Inicializa os dataframes
    df_clientes = pd.DataFrame()
    df_itens_metricas = pd.DataFrame()

    try:
        # Lista todos os objetos na pasta input
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=input_prefix
        )

        # Verifica se existem objetos
        if 'Contents' not in response:
            print(f"Nenhum arquivo encontrado em {input_prefix}")

            return df_clientes, df_itens_metricas

        # Para cada arquivo na pasta input
        for obj in response['Contents']:
            file_key = obj['Key']

            # Verifica se é um arquivo parquet
            if file_key.endswith('.parquet'):
                try:
                    # Lê o arquivo parquet
                    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
                    file_name = file_key.split('/')[-1].lower()

                    if 'cliente' in file_name:
                        df_clientes = pd.read_parquet(io.BytesIO(response['Body'].read()))
                        print("Arquivo de clientes lido com sucesso!")
                    elif 'itens' in file_name:
                        df_itens_metricas = pd.read_parquet(io.BytesIO(response['Body'].read()))
                        print("Arquivo de itens lido com sucesso!")

                except Exception as e:
                    print(f"Erro ao ler arquivo {file_key}: {str(e)}")
                    continue

        return df_clientes, df_itens_metricas

    except Exception as e:
        print(f"Erro ao listar objetos do bucket: {str(e)}")
        raise


def transform_sales_dates_fe(df_cliente_transacao: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms sales date data by adding weekday and weekend information.

    :param df_cliente_transacao: DataFrame containing sales data
    :return: DataFrame with added date-related columns
    """
    # Create a copy to avoid modifying the original
    df_cliente_transacao = df_cliente_transacao.copy()

    # Convert to datetime and extract date
    df_cliente_transacao['data_venda'] = pd.to_datetime(df_cliente_transacao['data_venda']).dt.date

    # Add weekday information (0 = Monday)
    df_cliente_transacao['dia_compra'] = df_cliente_transacao['data_venda'].apply(datetime.weekday)

    # Add weekend flag (1 for Saturday/Sunday, 0 otherwise)
    df_cliente_transacao['fds'] = np.where(df_cliente_transacao['dia_compra'].isin([5, 6]), 1, 0)

    return df_cliente_transacao


def plot_weekday_sales_fe(df: pd.DataFrame) -> go.Figure:
    """
    Creates a bar plot showing the distribution of sales across weekdays.

    :param df: DataFrame with 'dia_compra' column (0-6 for Monday-Sunday)
    :return: Plotly figure
    """
    dias_semana = ['Segunda', 'Terça', 'Quarta', 'Quinta', 'Sexta', 'Sábado', 'Domingo']
    
    # Count sales by weekday
    weekday_counts = df['dia_compra'].value_counts().sort_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=dias_semana,
        y=weekday_counts,
        marker_color='red'
    ))
    
    fig.update_layout(
        title='Quantidade de Compras por Dia da Semana',
        xaxis_title='Dia da Semana',
        yaxis_title='Quantidade de Compras',
        plot_bgcolor='white',
        showlegend=False,
        width=800,
        height=500
    )
    
    # Update axes
    fig.update_xaxes(
        showgrid=False,
        zeroline=False
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=0.3,
        gridcolor='lightgrey',
        zeroline=False
    )

    return fig


def plot_weekend_sales_fe(df: pd.DataFrame) -> go.Figure:
    """
    Creates a bar plot showing the distribution of sales between weekdays and weekends.

    :param df: DataFrame with 'fds' column (0 for weekday, 1 for weekend)
    :return: Plotly figure
    """
    lst_fds = ['Dia Útil', 'Final de Semana']
    
    # Count sales by weekday/weekend
    fds_counts = df['fds'].value_counts().sort_index()
    
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=lst_fds,
        y=fds_counts,
        marker_color='red'
    ))
    
    fig.update_layout(
        title='Quantidade de Compras por Dia Útil e Final de Semana',
        yaxis_title='Quantidade de Compras',
        xaxis_title='',
        plot_bgcolor='white',
        showlegend=False,
        width=800,
        height=500
    )
    
    # Update axes
    fig.update_xaxes(
        showgrid=False,
        zeroline=False
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=0.3,
        gridcolor='lightgrey',
        zeroline=False
    )

    return fig


def process_customer_metrics_fe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Process customer transaction data to calculate various metrics per customer.
    
    :param df: DataFrame containing customer transaction data
    :return: DataFrame with calculated customer metrics
    """
    # Create a copy to avoid modifying original
    df = df.copy()
    
    # Converter 'data_venda' para datetime e ordenar por cliente e data de venda
    df['data_venda'] = pd.to_datetime(df['data_venda'])
    df = df.sort_values(by=['id_cliente', 'data_venda'])
    
    # Calcular intervalo de dias entre cada compra do cliente e preencher valores nulos com 0
    df['intervalo_compra'] = df.groupby('id_cliente')['data_venda'].diff().dt.days
    df['intervalo_compra'].fillna(0, inplace=True)
    
    # Create division dummy variables
    divisoes_encoded = pd.get_dummies(df['nome_divisao'])
    divisoes_encoded['id_cliente'] = df['id_cliente']
    divisoes_por_cliente = divisoes_encoded.groupby('id_cliente').sum().reset_index()
    
    # Calculate customer metrics
    df_metricas_cliente = df.groupby("id_cliente").agg(
        qtd_compras=('valor', 'size'),
        fds=('fds', 'sum'),
        dia_preferido=('dia_compra', lambda x: x.mode()[0]),
        compras_ON=('tipo_venda', lambda x: (x == 'ON').sum()),
        compras_OFF=('tipo_venda', lambda x: (x == 'OFF').sum()),
        ticket_medio=('valor', 'mean'),
        total_gasto=('valor', 'sum'),
        produtos_diferentes=('codigo_item', pd.Series.nunique),
        intervalo_medio=('intervalo_compra', 'mean')
    ).reset_index()
    
    # Merge metrics with divisions
    df_metricas_cliente = pd.merge(
        df_metricas_cliente, 
        divisoes_por_cliente, 
        on='id_cliente', 
        how='left'
    )
    
    return df_metricas_cliente


def plot_purchase_interval_fe(df: pd.DataFrame) -> go.Figure:
    """
    Creates a histogram showing the distribution of average purchase intervals.
    
    :param df: DataFrame containing 'intervalo_medio' column
    :return: Plotly figure with log-scale histogram
    """
    fig = go.Figure()

    fig.add_trace(go.Histogram(
        x=df['intervalo_medio'],
        nbinsx=100,
        marker=dict(
            color='red',
            line=dict(color='black', width=0.5)
        ),
        opacity=1,
        hoverinfo='skip'
    ))

    fig.update_layout(
        title={
            'text': 'Distribuição do Intervalo de Compra',
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 14}
        },
        xaxis_title='Intervalo de Compra [dias]',
        yaxis_title='Contagem',
        plot_bgcolor='white',
        width=800,
        height=500,
        showlegend=False,
        xaxis=dict(
            range=[-10, 300],
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False
        ),
        yaxis=dict(
            type='log',
            showgrid=True,
            gridwidth=0.3,
            gridcolor='rgba(128, 128, 128, 0.2)',
            zeroline=False,
            range=[0, 4.1]  # 10^0 to 10^4
        ),
        bargap=0.02
    )

    return fig
