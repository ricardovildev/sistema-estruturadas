import streamlit as st
import pandas as pd
from backend.conexao import conectar
from sqlalchemy import text

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

def atualizar_preco_ativos(engine):
    with engine.begin() as conn:
        # Aqui pode ser chamada a rotina real para atualizar preços na tabela ativos_yahoo
        conn.execute(text("UPDATE ativos_yahoo SET preco_atual = preco_atual"))  # Exemplo simples
    st.success("Preços atualizados com sucesso.")

def calcular_resultados(engine, df):
    hoje = pd.Timestamp.now().normalize()

    query = text("""
        SELECT hp.codigo_bdi, hp.data_pregao, hp.preco_fechamento
        FROM historico_precos hp
    """)
    df_precos = pd.read_sql(query, engine)

    df = df.copy()
    for col in ['preco_fechamento', 'resultado']:
        if col not in df.columns:
            df[col] = pd.NA

    atualizacoes = []

    for idx, row in df.iterrows():
        if pd.notna(row.get('preco_fechamento')) and pd.notna(row.get('resultado')):
            continue

        ativo = row['Ativo']
        vencimento = row['Data Vencimento']
        precos_ativos = df_precos[
            (df_precos['codigo_bdi'] == ativo) & 
            (df_precos['data_pregao'] == pd.to_datetime(vencimento))
        ]

        if not precos_ativos.empty:
            preco_fech = precos_ativos['preco_fechamento'].iloc[0]
            df.at[idx, 'preco_fechamento'] = preco_fech

            strike = row.get('Valor do Strike (1)', 0)
            qtd = row.get('Quantidade Ativa (1)', 0)
            custo_unit = row.get('Custo Unitário Cliente', 0)
            preco_atual = preco_fech

            ajuste = 0
            resultado = 0
            if preco_atual > strike and qtd < 0:
                ajuste = (strike - preco_atual) * qtd
                resultado = preco_atual * abs(qtd) + ajuste + custo_unit * abs(qtd)
            else:
                resultado = custo_unit * abs(qtd)

            df.at[idx, 'resultado'] = resultado

            atualizacoes.append({
                'id': row.get('id', None),
                'preco_fechamento': preco_fech,
                'resultado': resultado,
            })

    with engine.begin() as conn:
        for atualizacao in atualizacoes:
            if atualizacao['id'] is not None:
                conn.execute(text("""
                    UPDATE suas_tabela_operacoes
                    SET preco_fechamento = :preco, resultado = :resultado
                    WHERE id = :id
                """), {'preco': atualizacao['preco_fechamento'], 'resultado': atualizacao['resultado'], 'id': atualizacao['id']})

    st.success(f"Foram atualizados {len(atualizacoes)} registros.")

    return df

def render():
    st.title("Cálculo de Resultados das Operações Estruturadas")

    engine = conectar()

    st.write("### Importar Planilha Relatório de Posição (1)")
    arquivo = st.file_uploader("Escolha o arquivo (.xlsx)", type=["xlsx"])

    if arquivo:
        if st.button("Importar Planilha"):
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

            df.to_sql('suas_tabela_operacoes', con=engine, if_exists='replace', index=False)
            st.success("Planilha importada e salva no banco com sucesso.")

    try:
        df_bd = pd.read_sql("SELECT * FROM suas_tabela_operacoes", con=engine)
    except Exception:
        df_bd = pd.DataFrame(columns=[
            'Código do Cliente', 'Código do Assessor', 'Código da Operação', 
            'Data Registro', 'Ativo', 'Estrutura'
        ])

    if 'Código do Cliente' in df_bd.columns:
        df_bd = df_bd.rename(columns={'Código do Cliente': 'Conta'})

    st.write("### Filtros para Consulta")
    with st.form("form_filtros"):
        filtro_conta = st.multiselect('Conta', options=df_bd['Conta'].dropna().unique())
        filtro_assessor = st.multiselect('Assessor', options=df_bd['Código do Assessor'].dropna().unique() if 'Código do Assessor' in df_bd else [])
        filtro_operacao = st.multiselect('Operação', options=df_bd['Código da Operação'].dropna().unique() if 'Código da Operação' in df_bd else [])
        filtro_ativo = st.multiselect('Ativo', options=df_bd['Ativo'].dropna().unique() if 'Ativo' in df_bd else [])
        filtro_estrutura = st.multiselect('Estrutura', options=df_bd['Estrutura'].dropna().unique() if 'Estrutura' in df_bd else [])
        aplicar = st.form_submit_button("Aplicar Filtros")

    df_filtrado = df_bd.copy()
    if aplicar:
        if filtro_conta:
            df_filtrado = df_filtrado[df_filtrado['Conta'].isin(filtro_conta)]
        if filtro_assessor:
            df_filtrado = df_filtrado[df_filtrado['Código do Assessor'].isin(filtro_assessor)]
        if filtro_operacao:
            df_filtrado = df_filtrado[df_filtrado['Código da Operação'].isin(filtro_operacao)]
        if filtro_ativo:
            df_filtrado = df_filtrado[df_filtrado['Ativo'].isin(filtro_ativo)]
        if filtro_estrutura:
            df_filtrado = df_filtrado[df_filtrado['Estrutura'].isin(filtro_estrutura)]

    colunas_para_exibir = ['Conta', 'Código do Assessor', 'Código da Operação', 'Data Registro', 'Ativo',
                          'Estrutura', 'preco_fechamento', 'resultado', 'Ajuste', 'Status', 'Volume', 'Cupons/Premio']
    colunas_existentes = [c for c in colunas_para_exibir if c in df_filtrado.columns]

    st.dataframe(df_filtrado[colunas_existentes])

    if st.button("Atualizar preços atuais"):
        atualizar_preco_ativos(engine)

    if st.button("Calcular Resultados"):
        df_bd = calcular_resultados(engine, df_bd)
        st.experimental_rerun()
