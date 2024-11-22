import streamlit as st
from st_renner_libs import *

def main():
    # Cria o título da página
    st.markdown('<h1 style="color: #FF0000;">Renner ReThink.</h1>', unsafe_allow_html=True)
    st.markdown("<h3 style='color: #FF0000;'>Feature Engineering</h3>", unsafe_allow_html=True)
    st.markdown("<h4 style='color: #FF0000;'>Criação de atributos e registros</h4>", unsafe_allow_html=True)

    _, df_transacao = read_csv_files_fe()
    _, df_itens_metricas = read_parquet_files_fe()
    df_cliente_transacao = df_transacao.merge(df_itens_metricas)
    df_cliente_transacao = transform_sales_dates_fe(df_cliente_transacao)
    fig1 = plot_weekday_sales_fe(df_cliente_transacao)
    fig2 = plot_weekend_sales_fe(df_cliente_transacao)
    df_metricas_cliente = process_customer_metrics_fe(df_cliente_transacao)
    fig3 = plot_purchase_interval_fe(df_metricas_cliente)

    st.markdown('''
        Foi verificado que um mesmo item possuía diversos valores de venda e para tentar entender melhor esse comportamento, criamos um dataset auxiliar em que capturamos o preço mínimo, médio, máximo e a moda do preço para cada item. 
        Com essas informações, pudemos calcular o desvio padrão, amplitude e coeficiente de variação de cada um dos itens, totalizando 186.127 itens.
    ''')

    col1, col2, col3 = st.columns(3)

    with col1:
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown('''
            **Vendas por Dia da Semana**
            - Distribuição das vendas ao longo dos dias úteis
            - Identificação dos dias com maior volume de vendas
        ''')

    with col2:
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('''
            **Vendas aos Fins de Semana**
            - Análise específica das vendas em sábados e domingos
            - Comparação com o padrão de dias úteis
        ''')

    with col3:
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown('''
            **Intervalo entre Compras**
            - Distribuição do tempo entre compras consecutivas
            - Identificação de padrões de frequência
            - Análise da regularidade dos clientes
        ''')

    st.markdown("<h4 style='color: #FF0000;'>Regras de Limpeza Aplicadas</h4>", unsafe_allow_html=True)
    st.markdown('''
        Com objetivo de reduzir outliers, aplicamos as seguintes regras em sequência:
        - Descartar Itens com moda do preço menor do que R$1,00
        - Descartar itens com desvio padrão calculado a partir de 1,5
    ''')

    st.markdown("<h4 style='color: #FF0000;'>Métricas Criadas</h4>", unsafe_allow_html=True)
    st.markdown('''
        Na base de clientes criamos métricas para cada indivíduo:
        - Contagem de compras por modalidade e tipo de produto
        - Diferença entre primeira e última compra
        - Total de compras e tempo médio entre compras
        - Ticket médio
        - Classificação de cliente (capital ou não)
        
        Para os registros de navegação, aplicamos dados de contagem de cada evento por cliente.
    ''')

    st.markdown("<h4 style='color: #FF0000;'>Integração de dados</h4>", unsafe_allow_html=True)
    st.markdown('''
        Como recebemos bases com 3 perspectivas diferentes (Cliente, Navegação, Transação), 
        utilizamos como chave primária em todas elas o campo id_cliente, dessa forma, 
        foi possível unir todas as tabelas e criar um dataset completo com foco nos clientes, 
        seus dados e características agregadas.

        Após as junções, totalizamos 23.664 clientes diferentes.
    ''')
