import streamlit as st

def main():
    # Cria o título da página
    st.markdown('<h1 style="color: #FF0000;">Renner ReThink.</h1>', unsafe_allow_html=True)

    # Cria o título da seção
    st.markdown("<h3 style='color: #FF0000;'>Background</h3>", unsafe_allow_html=True)

    # Cria o texto da página
    texto_background = '''
        - Mercado de e-commerce cada vez mais concorrido:
            - Maior oferta e diversidade de opções 
            - Clientes podem rapidamente encontrar informações relevantes sobre suas demandas
        - Entender o comportamento do público
        - Definir estratégias competitivas
        - Ser acurado na identificação de campanhas atrativas para diferentes grupos 
            - Peça-chave na prospecção e fidelização de clientes 

        - Grupo Renner: um dos players mais importantes no país
            - Busca ser referência no uso de dados para inteligência de mercado
            - Alinhando tecnologia aos produtos de moda, decoração, financeiro e serviços
    '''

    # Mostra o texto na página
    st.markdown(texto_background)

    # Cria o sub-título da seção
    st.markdown("<h4 style='color: #FF0000;'>Objetivos de Negócio</h4>", unsafe_allow_html=True)

    # Cria o texto da página
    texto_objetivos_negocio = '''        
        - Encontrar agrupamentos de clientes com base no estágio de vida e padrão de consumo
        - Buscar identificar quais produtos e canais são mais aderentes ao seus gostos
        - Hipótese: estes segmentos refletem necessidades e preferências distintas, permitindo:
            - Campanhas de marketing mais direcionadas e personalizadas
            - Estratégias de vendas otimizadas
    '''

    # Mostra o texto na página
    st.markdown(texto_objetivos_negocio)

    # Cria o sub-título da seção
    st.markdown("<h4 style='color: #FF0000;'>Critérios de Sucesso</h4>", unsafe_allow_html=True)

    # Cria o texto da página
    texto_criterios_negocio = '''
        - Técnico:
            - Métricas consolidadas para avaliação dos clusters
            - Capacidade de representar segmentos distintos de clientes baseado nas características comuns entre indivíduos de um mesmo cluster. 

        - Negócio: 
            - Clusters explicáveis e coerentes, com caracterização clara de similaridade e diferenciação. 
            - Resultados e características dos clusters coerentes e alinhados com as expectativas da Renner. 
        '''

    # Mostra o texto na página
    st.markdown(texto_criterios_negocio)