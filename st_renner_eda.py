import streamlit as st
from st_renner_libs import *


def main():
    # Page title
    st.markdown('<h1 style="color: #FF0000;">Renner ReThink.</h1>', unsafe_allow_html=True)
    st.markdown("<h3 style='color: #FF0000;'>Análise Exploratória</h3>", unsafe_allow_html=True)

    # Introduction text
    texto_analise_exp = '''
        - Objetivo principal: entender a distribuição, características e particularidades dos dados
        - Para garantir um melhor entendimento e possibilitando o grupo a escolher as melhores abordagens para serem aplicadas tanto na preparação, quanto na modelagem do problema. 

        Abaixo, algumas observações feitas pelo grupo:
    '''
    st.markdown(texto_analise_exp)

    # Load and prepare data
    df_clientes, df_navegacao, df_transacao = read_csv_files_eda()
    df_clientes = converte_data_clientes(df_clientes)
    df_clientes = aplicar_limpeza_cidades(df_clientes)
    
    # Create all figures first
    fig1 = grafico_capitais_interior(df_clientes)
    fig2 = criar_grafico_distribuicao_idade(df_clientes)
    fig3, _ = criar_grafico_distribuicao_idades_negativas(df_clientes)
    df_clientes = df_clientes.loc[df_clientes['idade']>=16]
    fig4 = criar_grafico_distribuicao_genero(df_clientes)
    fig5 = criar_grafico_distribuicao_compras(df_clientes)
    fig6 = criar_grafico_intervalo_compras(df_clientes)
    cidades_35_percent_data, percentual_acumulado_35 = transformacoes_grafico_cidades(df_clientes)
    fig7 = criar_grafico_cidades_35_percent(cidades_35_percent_data, percentual_acumulado_35)
    fig8 = criar_grafico_eventos_jornada(df_navegacao)
    fig9 = criar_grafico_tipo_venda(df_transacao)
    fig10 = criar_grafico_boxplot_divisao(df_transacao)
    fig11 = plot_sales_value_distribution(df_transacao)
    df_vendas_item = transformacao_grafico_vendas_item(df_transacao)
    fig12 = plot_top_items_sales(df_vendas_item, top_n=10)
    fig13 = plot_item_boxplot(df_transacao, 108799)
    df_variacao = transformacoes_grafico_variacao(df_transacao)
    fig14 = plot_cv_distribution(df_variacao)

    # Block 1: Distribution of capitals vs interior and age distribution
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown("Análise da distribuição de clientes entre capitais e interior.")
    with col2:
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('''
            Distribuição das idades dos clientes, mostrando a concentração
            em diferentes faixas etárias.
        ''')

    # Block 2: Age analysis and gender distribution
    col3, col4 = st.columns(2)
    with col3:
        st.plotly_chart(fig3, use_container_width=True)
        st.markdown('''
            - Casos com idade negativa
            - Análise dos valores indicou que podem ser facilmente tratados 
              tomando-se o valor absoluto das idades
        ''')
    with col4:
        st.plotly_chart(fig4, use_container_width=True)
        st.markdown('''
            - Grande predominância de clientes do sexo feminino, o que era 
              esperado considerando as tendências de clientes do negócio 
              da organização parceira
        ''')

    # Block 3: Purchase distribution and intervals
    col5, col6 = st.columns(2)
    with col5:
        st.plotly_chart(fig5, use_container_width=True)
        st.markdown("Análise da distribuição de compras entre os clientes.")
    with col6:
        st.plotly_chart(fig6, use_container_width=True)
        st.markdown("Análise dos intervalos entre compras dos clientes.")

    # Block 4: Cities analysis and navigation events
    col7, col8 = st.columns(2)
    with col7:
        st.plotly_chart(fig7, use_container_width=True)
        st.markdown('''
            - São Paulo e Rio de Janeiro representam 20% do número total de clientes
            - Adicionando-se Porto Alegre e Brasília obtém-se pouco mais de 25%, 
              o que demonstra como as duas primeiras cidades possuem grande 
              representatividade no dataset
        ''')
    with col8:
        st.plotly_chart(fig8, use_container_width=True)
        st.markdown('''
            - Predominante a presença dos eventos 'view_item' e 'select_item', 
              tendo os demais eventos poucos registros
        ''')

    # Block 5: Sales type and value distribution
    col9, col10 = st.columns(2)
    with col9:
        st.plotly_chart(fig9, use_container_width=True)
        st.markdown('''
            - Grande diferença de registros quando comparadas compras Online e Offline
            - Vendas Online representam em torno de 1/3 do total de vendas 
        ''')
    with col10:
        st.plotly_chart(fig10, use_container_width=True)
        st.markdown('''
            - Os valores das vendas por categoria têm certo equilíbrio nos dados
            - Porém, ainda existem muitos outliers que deverão ser tratados 
        ''')

    # Block 6: Sales distribution and top items
    col11, col12 = st.columns(2)
    with col11:
        st.plotly_chart(fig11, use_container_width=True)
        st.markdown("Distribuição dos valores de venda")
    with col12:
        st.plotly_chart(fig12, use_container_width=True)
        st.markdown('''
            - Avaliando os 10 itens com maior valor total em vendas, podemos 
              perceber que há um item com Código 108799 que possui valor total 
              acima de 2 milhões de reais
            - O segundo item que mais vende (Código 77850) possui um total de 
              pouco mais de 115 mil reais
            - Supõe-se que possa haver algum tipo de erro para o primeiro caso 
              que deverá ser tratado
        ''')

    # Block 7: Item analysis and variation
    col13, col14 = st.columns(2)
    with col13:
        st.plotly_chart(fig13, use_container_width=True)
        st.markdown("Análise detalhada do item 108799")
    with col14:
        st.plotly_chart(fig14, use_container_width=True)
        st.markdown("Distribuição do coeficiente de variação nas vendas")