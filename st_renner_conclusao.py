import streamlit as st

def main():
    # Cria o título da página
    st.markdown('<h1 style="color: #FF0000;">Renner ReThink.</h1>', unsafe_allow_html=True)
    st.markdown("<h3 style='color: #FF0000;'>Conclusão</h3>", unsafe_allow_html=True)

    texto_conclusao = '''
        O projeto foi marcado por boa integração entre os membros da equipe e divisão equilibrada de tarefas. Apesar de um atraso inicial na disponibilização dos dados pela empresa parceira, que impactou o cronograma, os insights obtidos nas reuniões foram valiosos para alcançar os resultados esperados.

        Do ponto de vista técnico, a equipe possuía os conhecimentos necessários para implementação, embora houvesse desafios na validação e visualização dos resultados da clusterização. O projeto foi executado sem necessidade de recursos externos, mesmo com o RFM sendo uma técnica específica para comércio e varejo.

        Pontos de destaque do projeto:
        - Desenvolvimento de uma aplicação web acessível externamente
        - Criação de identidade visual própria com nome e logotipo padronizados
        - Implementação bem fundamentada do RFM, alinhada com as práticas da Renner
        - Resultados satisfatórios, embora com potencial para ajustes nos parâmetros

        Para evolução futura, identificou-se a necessidade de:
        - Refinamento dos parâmetros de modelagem para classificação mais precisa
        - Implementação de pós-processamento para identificar similaridades entre clientes não classificados
        - Desenvolvimento de insights sobre clientes com maior potencial de captação/retenção
    '''
    st.markdown(texto_conclusao)
