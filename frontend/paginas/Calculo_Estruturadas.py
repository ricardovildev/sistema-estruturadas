import streamlit as st
import pandas as pd
from backend.conexao import conectar

def converter_virgula_para_float(df, colunas):
    for col in colunas:
        df[col] = df[col].astype(str).str.replace('.', '').str.replace(',', '.').astype(float)
    return df

def identificar_opcao(df, seq=1):
    qtd_col = f'Quantidade Ativa ({seq})'
    tipo_col = f'Tipo ({seq})'
    strike_col = f'Valor do Strike ({seq})'
    df[f'opcao_call_comprada_{seq}'] = (df[qtd_col] > 0) & (df[tipo_col] == 'Call Option')
    df[f'opcao_call_vendida_{seq}'] = (df[qtd_col] < 0) & (df[tipo_col] == 'Call Option')
    df[f'opcao_put_comprada_{seq}'] = (df[qtd_col] > 0) & (df[tipo_col] == 'Put Option')
    df[f'opcao_put_vendida_{seq}'] = (df[qtd_col] < 0) & (df[tipo_col] == 'Put Option')
    df[f'strike_{seq}'] = df[strike_col]
    return df

def tratar_quantidade(row):
    if row['Quantidade Ativa (1)'] != 0:
        return abs(row['Quantidade Ativa (1)'])
    for i in range(1, 5):
        qt_col = f'Quantidade Ativa ({i})'
        tipo_col = f'Tipo ({i})'
        if qt_col in row and tipo_col in row:
            if row[qt_col] > 0 and str(row[tipo_col]).strip().lower() == 'stock':
                return abs(row[qt_col])
    return 0

def calcular_financiamento(df, precos_ativos):
    df = df.copy()
    df['preco_atual'] = df['Ativo'].map(precos_ativos)

    cond_call_vendida = df['opcao_call_vendida_1']
    strike = df.loc[cond_call_vendida, 'strike_1']
    qtd = df.loc[cond_call_vendida, 'Quantidade Ativa (1)']
    custo_unit = df.loc[cond_call_vendida, 'Custo Unitário Cliente']

    ajuste = pd.Series(0, index=df.index)
    status = pd.Series('', index=df.index)
    volume = pd.Series(0, index=df.index)
    cupons_premio = custo_unit * qtd.abs()

    ajuste_calc = (strike - df.loc[cond_call_vendida, 'preco_atual']) * qtd
    ajuste.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike)] = ajuste_calc.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike)]

    status.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike)] = 'Ação Vendida'
    volume.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike)] = df.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike), 'preco_atual'] * qtd + ajuste.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike)] + cupons_premio.loc[cond_call_vendida & (df.loc[cond_call_vendida, 'preco_atual'] > strike)]

    df['Ajuste'] = ajuste
    df['Status'] = status
    df['Volume'] = volume
    df['Cupons/Premio'] = cupons_premio
    return df

def render():
    st.title("Cálculo de Resultados das Operações Estruturadas")

    engine = conectar()

    arquivo = st.file_uploader("📥 Selecione o arquivo Relatório de Posição (1) (.xlsx)", type=["xlsx"])
    if arquivo:
        df = pd.read_excel(arquivo, dtype=str)

        colunas_numericas = [
            'Valor Ativo', 'Custo Unitário Cliente', 'Comissão Assessor',
            'Quantidade Ativa (1)', 'Quantidade Boleta (1)', '% do Strike (1)', 'Valor do Strike (1)',
            '% da Barreira (1)', 'Valor da Barreira (1)', 'Valor do Rebate (1)',
            'Quantidade Ativa (2)', 'Quantidade Boleta (2)', '% do Strike (2)', 'Valor do Strike (2)',
            '% da Barreira (2)', 'Valor da Barreira (2)', 'Valor do Rebate (2)',
            'Quantidade Ativa (3)', 'Quantidade Boleta (3)', '% do Strike (3)', 'Valor do Strike (3)',
            '% da Barreira (3)', 'Valor da Barreira (3)', 'Valor do Rebate (3)',
            'Quantidade Ativa (4)', 'Quantidade Boleta (4)', '% do Strike (4)', 'Valor do Strike (4)',
            '% da Barreira (4)', 'Valor da Barreira (4)', 'Valor do Rebate (4)'
        ]
        df = converter_virgula_para_float(df, colunas_numericas)

        df['Data Registro'] = pd.to_datetime(df['Data Registro'], dayfirst=True, errors='coerce')
        df['Data Vencimento'] = pd.to_datetime(df['Data Vencimento'], dayfirst=True, errors='coerce')

        df['Quantidade'] = df.apply(tratar_quantidade, axis=1)

        for i in range(1, 5):
            df = identificar_opcao(df, i)

        # Buscar preços atuais da tabela ativos_yahoo
        precos_df = pd.read_sql("SELECT asset_original, preco_atual FROM ativos_yahoo", engine)
        precos_ativos = dict(zip(precos_df['asset_original'], precos_df['preco_atual']))

        # Calcular financiamento
        df = calcular_financiamento(df, precos_ativos)

        # Filtros simples
        filtro_cliente = st.multiselect('Cliente', df['Código do Cliente'].unique())
        filtro_assessor = st.multiselect('Assessor', df['Código do Assessor'].unique())
        filtro_estrutura = st.multiselect('Estrutura', df['Estrutura'].unique())
        filtro_ativo = st.multiselect('Ativo', df['Ativo'].unique())
        filtro_data_registro = st.date_input('Período Data Registro', [df['Data Registro'].min(), df['Data Registro'].max()])
        filtro_data_vencimento = st.date_input('Período Data Vencimento', [df['Data Vencimento'].min(), df['Data Vencimento'].max()])

        df_filtrado = df.copy()
        if filtro_cliente:
            df_filtrado = df_filtrado[df_filtrado['Código do Cliente'].isin(filtro_cliente)]
        if filtro_assessor:
            df_filtrado = df_filtrado[df_filtrado['Código do Assessor'].isin(filtro_assessor)]
        if filtro_estrutura:
            df_filtrado = df_filtrado[df_filtrado['Estrutura'].isin(filtro_estrutura)]
        if filtro_ativo:
            df_filtrado = df_filtrado[df_filtrado['Ativo'].isin(filtro_ativo)]

        df_filtrado = df_filtrado[
            (df_filtrado['Data Registro'] >= pd.to_datetime(filtro_data_registro[0])) &
            (df_filtrado['Data Registro'] <= pd.to_datetime(filtro_data_registro[1])) &
            (df_filtrado['Data Vencimento'] >= pd.to_datetime(filtro_data_vencimento[0])) &
            (df_filtrado['Data Vencimento'] <= pd.to_datetime(filtro_data_vencimento[1]))
        ]

        st.dataframe(df_filtrado)
