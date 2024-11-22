import streamlit as st
import pandas as pd

def main():
    # Cria o título da página
    st.markdown('<h1 style="color: #FF0000;">Renner ReThink.</h1>', unsafe_allow_html=True)
    st.markdown("<h3 style='color: #FF0000;'>Modelagem</h3>", unsafe_allow_html=True)
    st.markdown("<h4 style='color: #FF0000;'>Técnicas e Suposições</h4>", unsafe_allow_html=True)

    texto_modelagem = '''
        Baseando-se no artigo "A review on customer segmentation methods for personalized customer targeting in e-commerce use cases" [link](https://link.springer.com/article/10.1007/s10257-023-00640-4), optamos pela segmentação RFM por sua robustez e ampla utilização. 
        Essa técnica será usada como baseline, pois é facilmente reproduzível com os dados da Renner, permitindo comparações com outros métodos.
    '''
    st.markdown(texto_modelagem)

    st.markdown("<h4 style='color: #FF0000;'>Avaliação de Score RFM</h4>", unsafe_allow_html=True)

    texto_modelagem2 = '''
    Como uma das estratégias de clusterização, utilizamos o score **RFM** para dividir os consumidores em perfis de consumo.
    Amplamente adotado no marketing e na literatura de segmentação de clientes, o RFM avalia:
    - **Recência**: Tempo desde a última compra.
    - **Frequência**: Número de compras realizadas.
    - **Monetário**: Valor gasto pelo cliente.

    Os scores de **Frequência** e **Monetário** foram definidos com base em quantis, permitindo uma melhor distribuição de perfis. Já a **Recência** utilizou intervalos de 3 meses, que mostraram-se eficazes segundo análise empírica, alinhando-se à expectativa de compras em um e-commerce de moda.
    '''
    st.markdown(texto_modelagem2)

    data = {
        "Score": [5, 4, 3, 2, 1],
        "Recência (em dias)": ["0-90)", "[90-180]", "[180-270]", "[270-365]", "(365-"],
        "Frequência (em quantidade de compras)": ["(8-", "[6-8]", "[4-6]", "[4-2]", "0-2)"],
        "Monetário (em reais)": ["(175-", "[150-175]", "[125-150]", "[100-125]", "0-100)"]
    }
    df = pd.DataFrame(data)
    st.table(df)

    texto_modelagem = '''
    A partir da geração de scores para as métricas de Recência, Frequência e Monetário, é possível criar clusters de clientes, baseando-se na metodologia do OmniConverter. Esses clusters ajudam no desenvolvimento de estratégias de marketing e fidelização. Abaixo, os principais grupos:

    - **Soulmates**: Clientes ideais, alta frequência e valor monetário. Priorize experiências positivas e atendimento especial.
    - **Lovers**: Promissores, com potencial de virar Soulmates. Aumente a confiança e frequência de compras.
    - **New Passions**: Novos clientes com alto potencial. Implementar onboarding e resolver problemas rapidamente pode evitar perdas.
    - **Flirting**: Novos clientes com menor frequência e valor. Estratégias de engajamento podem transformar em clientes leais.
    - **Apprentice**: Compraram 1-2 vezes com baixo valor. Abordagem com conversas para entender suas necessidades.
    - **Platonic Friends**: Ativos, fazem compras moderadas de valor médio.
    - **Potential Lovers**: Novos clientes com potencial de virarem Soulmates.
    - **About to Dump You**: Clientes inativos, é importante identificar motivos de afastamento e engajá-los novamente.
    - **Ex Lovers**: Clientes antigos com alta frequência e valor, mas que abandonaram a marca. Feedback é essencial para reconquistar ou ajustar estratégias.
    - **Don Juan**: Compraram poucas vezes, mas de alto valor. Investigue motivos e busque reengajamento.
    - **Break-Ups**: Clientes de baixo valor e alta taxa de devolução. Aceitar o churn e focar em segmentos mais promissores.
    '''
    st.markdown(texto_modelagem)

    data = {
        "Grupo": [
            "Soulmates", "Lovers", "New Passions", "Flirting", "Apprentice",
            "Platonic Friends", "Potential Lovers", "About to Dump You", 
            "Ex Lovers", "Don Juan", "Break Ups"
        ],
        "Score de Recência": [
            "5", "4-5", "5", "4", "4", "3-4", "5", "2-3", "1", "1", "1"
        ],
        "Score de Frequência": [
            "5", "3-5", "1", "1", "1", "3", "1", "1-5", "5", "1", "2"
        ],
        "Score Monetário": [
            "5", "3-5", "4-5", "4", "1", "3-4", "5", "1-5", "5", "5", "1"
        ]
    }
    df = pd.DataFrame(data)
    st.table(df)

    st.markdown("<h4 style='color: #FF0000;'>Visualização dos Resultados</h4>", unsafe_allow_html=True)

    st.image("newplot.png")
    col1, col2 = st.columns([1, 2])
    with col2:
        st.markdown("""
            Visualização da distribuição dos diferentes perfis identificados pela análise RFM.
            Esta visualização nos permite entender melhor como os clientes estão distribuídos
            entre os diversos grupos.
        """)

    # Segunda imagem com texto explicativo
    st.image("newplot2.png")
    col3, col4 = st.columns([1, 2])
    with col4:
        st.markdown("""
            Visualização da representação dos diferentes perfis do RFM nas vendas.
        """)

    # Terceira imagem com texto explicativo
    st.image("newplot3.png")
    col5, col6 = st.columns([1, 2])
    with col6:
        st.markdown("""
            Visualização da representação dos diferentes perfis do RFM nas vendas por categoria.
        """)
    st.markdown("<br>", unsafe_allow_html=True)

    texto_adicional = '''
        Após a aplicação da clusterização RFM, identificou-se que aproximadamente 50% dos clientes não se encaixavam em nenhum grupo definido, formando o grupo "No Profile". Isso ocorre porque nem todas as possibilidades de scores RFM estão cobertas pelos perfis definidos no artigo consultado.

        Inicialmente, testou-se o algoritmo KNN para atribuir esses clientes ao grupo mais próximo, mas surgiram problemas de descaracterização dos grupos. Por exemplo, clientes com alto volume de vendas sendo classificados como "Apprentice", que é caracterizado por poucas vendas.

        A solução implementada foi um **KNN segmentado**, onde os clientes sem perfil foram divididos em três grandes grupos baseados nos scores RFM:
        - Clientes que não compram há bastante tempo (grupos alvo: Don Juan, Breakups, Ex Lovers)
        - Clientes novos (grupos alvo: Potential Lovers, Apprentice, New Passions, Flirting)
        - Clientes que consomem frequentemente (grupos alvo: Soulmates, Lovers, Platonic Friends)

        Na avaliação dos modelos, o RFM mostrou resultados coerentes com os objetivos do projeto, enquanto as abordagens por KMeans e DBSCAN não produziram boa separação de clusters, resultando em muitos dados categorizados como ruído. A modelagem por RFM foi considerada a abordagem mais efetiva, tanto pelos experimentos realizados quanto pelo feedback da empresa parceira.
    '''
    st.markdown(texto_adicional)