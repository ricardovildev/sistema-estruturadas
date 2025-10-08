import streamlit as st
import pandas as pd
from backend.conexao import conectar
from sqlalchemy import text
from datetime import datetime

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
        return abs(float(row['Quantidade Ativa (1)']))
    for i in range(1, 5):
        qt_col = f'Quantidade Ativa ({i})'
        tipo_col = f'Tipo ({i})'
        if qt_col in row and tipo_col in row:
            if float(row[qt_col]) > 0 and str(row[tipo_col]).strip().lower() == 'stock':
                return abs(float(row[qt_col]))
    return 0

def atualizar_preco_ativos(engine):
    with engine.begin() as conn:
        # Atualiza preço para vencimentos futuros com join na tabela ativos_yahoo (ajustado para coluna asset_original)
        conn.execute(
            text("""
                UPDATE operacoes_estruturadas oe
                JOIN ativos_yahoo ay ON UPPER(TRIM(oe.Ativo)) = UPPER(TRIM(ay.asset_original))
                SET oe.preco_atual = ay.preco_atual
                WHERE oe.Data_Vencimento > CURDATE()
            """)
        )

        # Atualiza preço de fechamento para vencimentos passados com join na tabela historico_precos
        conn.execute(
            text("""
                UPDATE operacoes_estruturadas oe
                JOIN historico_precos hp ON UPPER(TRIM(oe.Ativo)) = UPPER(TRIM(hp.codigo_bdi))
                AND oe.Data_Vencimento = hp.data_pregao
                SET oe.preco_fechamento = hp.preco_ultimo
                WHERE oe.Data_Vencimento <= CURDATE()
            """)
        )
    st.success("Preços atualizados conforme a data de vencimento.")



def calcular_resultados(engine, df):
    hoje = datetime.today().date()
    df = df.copy()
    atualizacoes = []

    for idx, row in df.iterrows():
        estrutura = str(row.get('Estrutura', '')).strip().upper()
        vencimento_raw = row.get('Data_Vencimento')
        if pd.isna(vencimento_raw):
            continue  # pula registro com data vencimento inválida/nula

        vencimento = pd.to_datetime(vencimento_raw).date()

        preco = None
        if vencimento > hoje:
            preco = row.get('preco_atual', None)
        else:
            preco = row.get('preco_fechamento', None)

        if preco in [None, ''] or pd.isna(preco):
            continue

        quantidade = float(row.get('Quantidade', 0))
        valor_ativo = float(row.get('Valor_Ativo', 0))
        custo_unit = float(row.get('Custo_Unitario_Cliente', 0))
        strike_call_vendida = float(row.get('Valor_Strike_1', 0))
        dividendos = float(row.get('Dividendos', 0) if row.get('Dividendos', 0) not in [None, ''] else 0)

        cupons_premio = quantidade * custo_unit if (quantidade and custo_unit) else 0
        ajuste = 0
        resultado = 0
        status = ""

        if estrutura == "FINANCIAMENTO":
            if preco > strike_call_vendida:
                ajuste = (strike_call_vendida - preco) * quantidade
                resultado = (preco - valor_ativo + dividendos) * quantidade + ajuste + cupons_premio
                status = "Ação Vendida"
            else:
                resultado = cupons_premio
                ajuste = 0
                status = "Ação Vendida"
        else:
            resultado = cupons_premio

        volume = quantidade * preco if (quantidade and preco) else 0
        investido = quantidade * valor_ativo if (quantidade and valor_ativo) else 0
        percentual = resultado / investido if investido else 0

        atualizacoes.append({
            'id': row.get('id', None),
            'resultado': resultado,
            'Ajuste': ajuste,
            'Status': status,
            'Volume': volume,
            'Cupons_Premio': cupons_premio,
            'Percentual': percentual
        })

    with engine.begin() as conn:
        for a in atualizacoes:
            if a['id'] is not None:
                conn.execute(text("""
                    UPDATE operacoes_estruturadas
                    SET resultado = :resultado, Ajuste = :Ajuste, Status = :Status, Volume = :Volume, Cupons_Premio = :Cupons_Premio, Percentual = :Percentual
                    WHERE id = :id
                """), a)
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

            renomear_colunas = {
                'Código do Cliente': 'Conta',
                'Código do Assessor': 'Assessor',
                'Código da Operação': 'Codigo_da_Operacao',
                'Data Registro': 'Data_Registro',
                'Data Vencimento': 'Data_Vencimento',
                'Valor Ativo': 'Valor_Ativo',
                'Custo Unitário Cliente': 'Custo_Unitario_Cliente',
                'Comissão Assessor': 'Comissao_Assessor',
                'Quantidade Ativa (1)': 'Quantidade_Ativa_1',
                'Quantidade Boleta (1)': 'Quantidade_Boleta_1',
                'Tipo (1)': 'Tipo_1',
                '% do Strike (1)': 'Percentual_Strike_1',
                'Valor do Strike (1)': 'Valor_Strike_1',
                '% da Barreira (1)': 'Percentual_Barreira_1',
                'Valor da Barreira (1)': 'Valor_Barreira_1',
                'Valor do Rebate (1)': 'Valor_Rebate_1',
                'Tipo da Barreira (1)': 'Tipo_Barreira_1',
                'Quantidade Ativa (2)': 'Quantidade_Ativa_2',
                'Quantidade Boleta (2)': 'Quantidade_Boleta_2',
                'Tipo (2)': 'Tipo_2',
                '% do Strike (2)': 'Percentual_Strike_2',
                'Valor do Strike (2)': 'Valor_Strike_2',
                '% da Barreira (2)': 'Percentual_Barreira_2',
                'Valor da Barreira (2)': 'Valor_Barreira_2',
                'Valor do Rebate (2)': 'Valor_Rebate_2',
                'Tipo da Barreira (2)': 'Tipo_Barreira_2',
                'Quantidade Ativa (3)': 'Quantidade_Ativa_3',
                'Quantidade Boleta (3)': 'Quantidade_Boleta_3',
                'Tipo (3)': 'Tipo_3',
                '% do Strike (3)': 'Percentual_Strike_3',
                'Valor do Strike (3)': 'Valor_Strike_3',
                '% da Barreira (3)': 'Percentual_Barreira_3',
                'Valor da Barreira (3)': 'Valor_Barreira_3',
                'Valor do Rebate (3)': 'Valor_Rebate_3',
                'Tipo da Barreira (3)': 'Tipo_Barreira_3',
                'Quantidade Ativa (4)': 'Quantidade_Ativa_4',
                'Quantidade Boleta (4)': 'Quantidade_Boleta_4',
                'Tipo (4)': 'Tipo_4',
                '% do Strike (4)': 'Percentual_Strike_4',
                'Valor do Strike (4)': 'Valor_Strike_4',
                '% da Barreira (4)': 'Percentual_Barreira_4',
                'Valor da Barreira (4)': 'Valor_Barreira_4',
                'Valor do Rebate (4)': 'Valor_Rebate_4',
                'Tipo da Barreira (4)': 'Tipo_Barreira_4',
            }
            df = df.rename(columns=renomear_colunas)

            # Conversão segura para numérico das colunas importadas
            df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)
            df['Valor_Ativo'] = pd.to_numeric(df['Valor_Ativo'], errors='coerce').fillna(0)
            df['Investido'] = df['Quantidade'] * df['Valor_Ativo']

            if 'preco_atual' not in df.columns:
                df['preco_atual'] = None
            if 'preco_fechamento' not in df.columns:
                df['preco_fechamento'] = None

            colunas_tabela = [
                'Conta', 'Cliente', 'Assessor', 'Codigo_da_Operacao', 'Data_Registro',
                'Ativo', 'Estrutura', 'Valor_Ativo', 'Data_Vencimento', 'Custo_Unitario_Cliente',
                'Comissao_Assessor', 'Quantidade_Ativa_1', 'Quantidade_Boleta_1', 'Tipo_1',
                'Percentual_Strike_1', 'Valor_Strike_1', 'Percentual_Barreira_1', 'Valor_Barreira_1',
                'Valor_Rebate_1', 'Tipo_Barreira_1', 'Quantidade_Ativa_2', 'Quantidade_Boleta_2', 'Tipo_2',
                'Percentual_Strike_2', 'Valor_Strike_2', 'Percentual_Barreira_2', 'Valor_Barreira_2',
                'Valor_Rebate_2', 'Tipo_Barreira_2', 'Quantidade_Ativa_3', 'Quantidade_Boleta_3', 'Tipo_3',
                'Percentual_Strike_3', 'Valor_Strike_3', 'Percentual_Barreira_3', 'Valor_Barreira_3',
                'Valor_Rebate_3', 'Tipo_Barreira_3', 'Quantidade_Ativa_4', 'Quantidade_Boleta_4', 'Tipo_4',
                'Percentual_Strike_4', 'Valor_Strike_4', 'Percentual_Barreira_4', 'Valor_Barreira_4',
                'Valor_Rebate_4', 'Tipo_Barreira_4', 'Quantidade', 'Investido', 'preco_atual', 'preco_fechamento',
                'resultado', 'Ajuste', 'Status', 'Volume', 'Cupons_Premio', 'Percentual'
            ]
            df = df[[col for col in colunas_tabela if col in df.columns]]

            df = df.where(pd.notnull(df), None)

            df.to_sql('operacoes_estruturadas', con=engine, if_exists='append', index=False)

            st.success("Planilha importada e inserida no banco com sucesso.")

    try:
        df_bd = pd.read_sql("SELECT * FROM operacoes_estruturadas", con=engine)
    except Exception:
        df_bd = pd.DataFrame(columns=[
            'Conta', 'Cliente', 'Assessor', 'Codigo_da_Operacao', 'Data_Registro', 'Ativo', 'Estrutura',
            'preco_atual', 'preco_fechamento', 'resultado', 'Ajuste', 'Status', 'Volume', 'Cupons_Premio', 'Percentual'
        ])

    st.write("### Filtros para Consulta")
    with st.form("form_filtros"):
        filtro_conta = st.multiselect('Conta', options=df_bd['Conta'].dropna().unique())
        filtro_cliente = st.multiselect('Cliente', options=df_bd['Cliente'].dropna().unique() if 'Cliente' in df_bd else [])
        filtro_assessor = st.multiselect('Assessor', options=df_bd['Assessor'].dropna().unique() if 'Assessor' in df_bd else [])
        filtro_ativo = st.multiselect('Ativo', options=df_bd['Ativo'].dropna().unique() if 'Ativo' in df_bd else [])
        filtro_estrutura = st.multiselect('Estrutura', options=df_bd['Estrutura'].dropna().unique() if 'Estrutura' in df_bd else [])
        aplicar = st.form_submit_button("Aplicar Filtros")

    df_filtrado = df_bd.copy()
    if aplicar:
        if filtro_conta:
            df_filtrado = df_filtrado[df_filtrado['Conta'].isin(filtro_conta)]
        if filtro_cliente:
            df_filtrado = df_filtrado[df_filtrado['Cliente'].isin(filtro_cliente)]
        if filtro_assessor:
            df_filtrado = df_filtrado[df_filtrado['Assessor'].isin(filtro_assessor)]
        if filtro_ativo:
            df_filtrado = df_filtrado[df_filtrado['Ativo'].isin(filtro_ativo)]
        if filtro_estrutura:
            df_filtrado = df_filtrado[df_filtrado['Estrutura'].isin(filtro_estrutura)]

    colunas_para_exibir = [
        'Conta', 'Cliente', 'Assessor', 'Codigo_da_Operacao', 'Data_Registro', 'Ativo', 'Estrutura',
        'preco_atual', 'preco_fechamento', 'resultado', 'Ajuste', 'Status', 'Volume', 'Cupons_Premio', 'Percentual'
    ]
    colunas_existentes = [c for c in colunas_para_exibir if c in df_filtrado.columns]
    st.dataframe(df_filtrado[colunas_existentes])

    if st.button("Atualizar preços atuais"):
        atualizar_preco_ativos(engine)

    if st.button("Calcular Resultados"):
        df_bd = calcular_resultados(engine, df_bd)
        st.experimental_rerun()
