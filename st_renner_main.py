import streamlit as st
import st_renner_descricao_projeto
import st_renner_compreensao_negocio
import st_renner_eda
import st_renner_etl
import st_renner_feature_engineering
import st_renner_modelagem
import st_renner_conclusao
from st_renner_libs import *

# Cria a navegação entre as páginas
pages = st.navigation({
    "Descrição e Compreensão do Projeto": [
        st.Page(st_renner_descricao_projeto.main,
                title="Descrição do Projeto",
                url_path="descricao_projeto"),
        st.Page(st_renner_compreensao_negocio.main,
                title="Compreensão de Negócio",
                url_path="compreensao_negocio"),
    ],
    "Exploração e Transformação": [
        st.Page(st_renner_eda.main,
                title="Análise Exploratória",
                url_path="eda"),
        st.Page(st_renner_etl.main,
                title="ETL",
                url_path="etl")
    ],
    "Criação e Modelagem": [
        st.Page(st_renner_feature_engineering.main,
                title="Feature Engineering",
                url_path="feature_engineering"),
        st.Page(st_renner_modelagem.main,
                title="Modelagem",
                url_path="modelagem")
    ],
    "Resultados": [
        st.Page(st_renner_conclusao.main,
                title="Conclusão",
                url_path="conclusao")
    ]}, position='sidebar')

# Configurações da página
st.set_page_config(page_title=f"Renner Rethink - {pages.title}",
                   layout='wide',
                   initial_sidebar_state='auto',
                   page_icon="renner-logo-6.png")


s3_client = get_s3_client()

# Cria o markdown para estilizar o conteúdo da página
st.markdown(
    """
    <style>
    .block-container {
    padding-top: 1.5rem;
    padding-bottom: 0rem;
    padding-left: 5rem;
    padding-right: 5rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Cria o markdown para estilizar o logo e o menu lateral
st.markdown(
    """
    <style>
    /* Titles in bright red */
    h1, h2, h3, h4, h5, h6 {
        color: #FF0000;  /* bright red */
    }

    [data-testid="stSidebarNav"] a {
        color: white !important;
    }

    /* Ensure text within the sidebar remains white */
    [data-testid="stSidebarNav"] a:hover {
        color: white !important;
    }

    /* Logo alignment and size adjustments */
    [data-testid="stSidebarHeader"] img[data-testid="stLogo"] {
        max-width: 100%;
        height: auto;
        width: 100%;
        margin-top: 5px;
    }

    /* Control button adjustments */
    [data-testid="stSidebarCollapseButton"] button {
        margin-top: 0px;
        margin-left: -25px;
    }

    [data-testid="collapsedControl"] > div > button {
        margin-top: 10px;
    }

    /* Collapsed logo size */
    [data-testid="collapsedControl"] img[data-testid="stLogo"] {
        width: 8rem;
        height: auto;
        max-width: 8rem;
        margin-top: 5px;
        margin-left: -10px;
    }
    </style>
    """, unsafe_allow_html=True)

# Função para criar o botão com estilo personalizado
def create_linkedin_button(link, name):
    return f"""
    <a href="{link}" target="_blank" style="text-decoration: none;">
        <button style="
            display: flex;
            align-items: center;
            background-color: #FF0000; 
            color: white; 
            border: none; 
            padding: 5px 3px; 
            font-size: 6px; 
            font-weight: bold;
            border-radius: 5px; 
            cursor: pointer;
            margin: -5px;
            width: 120%;
        ">
            <img src="https://upload.wikimedia.org/wikipedia/commons/c/ca/LinkedIn_logo_initials.png" alt="LinkedIn Logo" style="width: 10px; height: 10px; margin-right: 2px;">
            {name}
        </button>
    </a>
    """

# Criando quatro botões em quatro colunas
st.sidebar.markdown(
    f"""
    <div style="display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 12px;">
        {create_linkedin_button('https://www.linkedin.com/in/pedro-devincenzi-ferreira', 'Pedro Ferreira')}
        {create_linkedin_button('https://www.linkedin.com/in/luiz-e-souza', 'Luiz Eduardo')}
        {create_linkedin_button('https://www.linkedin.com/in/lorenzo-lazzarotto', 'Lorenzo Lazzarotto')}
        {create_linkedin_button('https://www.linkedin.com/in/lucas-gomes-nascimento', 'Lucas Gomes')}
    </div>
    """,
    unsafe_allow_html=True
)

# Espaço vazio para "empurrar" o botão para a parte de baixo
st.sidebar.markdown("<br>", unsafe_allow_html=True)

st.sidebar.markdown(
    """
    <a href="https://www.buymeacoffee.com/projetocdia24.2" target="_blank" style="text-decoration: none;">
        <button style="
            display: flex;
            align-items: center;
            justify-content: center;
            background-color: white; 
            color: #FF0000; 
            border: none; 
            padding: 10px; 
            font-size: 12px; 
            font-weight: bold;
            border-radius: 5px; 
            cursor: pointer;
            width: 100%;
        ">
            <span style="font-size: 16px; margin-right: 8px;">☕</span> Buy Us a Coffee
        </button>
    </a>
    """,
    unsafe_allow_html=True
)

# Adiciona o logo na página
st.logo("renner_rethink_logo.png", icon_image="renner_rethink_logo.png")

# Roda a aplicação
try:
    pages.run()
except Exception as e:
    st.error(f"Something went wrong: {str(e)}", icon=":material/error:")