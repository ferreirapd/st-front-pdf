import streamlit as st
from st_renner_libs import *

def main():
    st.markdown("<h1 style='color: #FF0000;'>Renner ReThink.</h1>", unsafe_allow_html=True)
    st.markdown("<h3 style='color: #FF0000;'>ETL</h3>", unsafe_allow_html=True)

    # Introduction text
    st.markdown('''
        Nesta seção, apresentamos as principais transformações e limpezas realizadas nos dados,
        bem como suas justificativas e impactos nas análises.
    ''')

    # Load and prepare data
    df_clientes, _, df_transacao = read_csv_files_eda()
    df_clientes = converte_data_clientes(df_clientes)

    # Create all figures first
    fig1 = plot_age_distribution_etl(df_clientes)
    fig2 = plot_age_distribution_over_16(df_clientes)
    fig3 = plot_purchase_interval(df_clientes)
    df_variacao = transformacoes_etl_heuristicas(df_transacao)
    fig4, df_variacao_m1 = plot_variation_coefficient(df_variacao)
    fig5, _ = plot_filtered_variation_coefficient(df_variacao_m1)
    fig6 = plot_filtered_variation_coefficient_restrictive(df_variacao_m1)

    # Block 1: Age distribution before and after correction
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig1, use_container_width=True)
    with col2:
        st.plotly_chart(fig2, use_container_width=True)
    st.markdown('''
        - Casos com idade negativa removidos
        - Remoção de idades inferiores a 16 anos
    ''')

    # Block 2: Purchase intervals and initial variation analysis
    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown('''
            - Verificamos 23 clientes cuja data da primeira compra na Renner 
              estava registrada como posterior a data da última compra
            - Fizemos a inversão dos registros para corrigir o problema
            - Distribuição dos intervalos entre compras após correção
        ''')
    with col4:
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown('''
            - Coeficiente de variação considerando apenas itens com preço 
              moda igual ou maior que R$ 1,00
        ''')

    # Block 3: Filtered variation coefficients
    col5, col6 = st.columns(2)
    with col5:
        st.plotly_chart(fig5, use_container_width=True)
        st.markdown('''
            - Coeficiente de variação com desvio padrão igual a 3
        ''')
    with col6:
        st.plotly_chart(fig6, use_container_width=True)
        st.markdown('''
            - Coeficiente de variação com desvio padrão igual a 1,5
        ''')
