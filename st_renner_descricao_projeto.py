import streamlit as st

def main():
    # Cria o título da página
    st.markdown('<h1 style="color: #FF0000;">Renner ReThink.</h1>', unsafe_allow_html=True)

    # Cria o título da seção
    st.markdown("<h3 style='color: #FF0000;'>Descrição do Projeto</h3>", unsafe_allow_html=True)

    # Cria o texto da página
    texto_descricao_problema = '''
        Com o crescente aumento do uso de e-commerce, diversidade de produtos e serviços disponíveis, e características distintas de clientes que utilizam este tipo de plataforma, estratégias que funcionavam de maneira eficiente tempos atrás, podem não responder tão bem às demandas atuais. 

        Conseguir identificar de maneira correta as diferenças fundamentais entre grupos de clientes e a partir disso, ser capaz de elaborar estratégias ideais para cada grupo é um desafio contemporâneo e dinâmico, mas que tem se tornado um diferencial importante na retenção e incremento de receitas. 
    '''

    # Mostra o texto na página
    st.markdown(texto_descricao_problema)

    # Cria o título da seção
    st.markdown("<h3 style='color: #FF0000;'>Descrição da Solução Proposta</h3>", unsafe_allow_html=True)

    # Cria o texto da página
    texto_descricao_solucao = '''
        Como proposta de trabalho, o grupo planeja desenvolver uma ferramenta chamada “Renner 
        Rethink”, que visa analisar cada cliente sob diversos prismas e agrupá-los através de técnicas de 
        Clusterização, para identificar os produtos mais relevantes a serem oferecidos para cada grupo de 
        clientes. 
        Os entregáveis serão feitos através de páginas web com as seguintes funções: 
        - Detalhamentos das análises e tratamento dos dados 
        - Modelagem interativa, onde a Renner pode executar testes e avaliar resultados sobre os modelos 
        - Relatório em PDF com toda a explicação do projeto baseado no CRISP-DM
    '''

    # Mostra o texto na página
    st.markdown(texto_descricao_solucao)