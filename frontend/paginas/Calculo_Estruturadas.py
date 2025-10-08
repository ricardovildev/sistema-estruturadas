import streamlit as st
import pandas as pd
from backend.conexao import conectar
from sqlalchemy import text
from datetime import datetime
import math

def converter_virgula_para_float(df, colunas):
    for col in colunas:
        df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
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

def safe_val(val):
    if isinstance(val, float) and (math.isnan(val) or val == float('nan')):
        return None
    return val

def atualizar_preco_ativos(engine):
    with engine.begin() as conn:
        # Atualiza preços para vencimentos futuros
        conn.execute(
            text("""
                UPDATE operacoes_estruturadas oe
                JOIN ativos_yahoo ay 
                    ON UPPER(TRIM(oe.Ativo)) = UPPER(TRIM(ay.asset_original))
                SET oe.preco_atual = ay.preco_atual
                WHERE oe.Data_Vencimento > CURDATE()
            """)
        )

        # Atualiza preços de fechamento para vencimentos passados
        conn.execute(
            text("""
                UPDATE operacoes_estruturadas oe
                JOIN historico_precos hp 
                    ON UPPER(TRIM(oe.Ativo)) = UPPER(TRIM(hp.codigo_bdi))
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

    for _, row in df.iterrows():
        estrutura = str(row.get('Estrutura', '')).strip().upper()
        vencimento_raw = row.get('Data_Vencimento')
        if pd.isna(vencimento_raw):
            continue
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
            'id': safe_val(row.get('id', None)),
            'resultado': safe_val(resultado),
            'Ajuste': safe_val(ajuste),
            'Status': status,
            'Volume': safe_val(volume),
            'Cupons_Premio': safe_val(cupons_premio),
            'Percentual': safe_val(percentual)
        })

    with engine.begin() as conn:
        for a in atualizacoes:
            if a['id'] is not None:
                conn.execute(text("""
                    UPDATE operacoes_estruturadas
                    SET resultado = :resultado, Ajuste = :Ajuste, Status = :Status, 
                        Volume = :Volume, Cupons_Premio = :Cupons_Premio, Percentual = :Percentual
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
            }
            df = df.rename(columns=renomear_colunas)

            df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)
            df['Valor_Ativo'] = pd.to_numeric(df['Valor_Ativo'], errors='coerce').fillna(0)
            df['Investido'] = df['Quantidade'] * df['Valor_Ativo']

            if 'preco_atual' not in df.columns:
                df['preco_atual'] = None
            if 'preco_fechamento' not in df.columns:
                df['preco_fechamento'] = None

            df = df.where(pd.notnull(df), None)
            df.to_sql('operacoes_estruturadas', con=engine, if_exists='append', index=False)
            st.success("Planilha importada e inserida no banco com sucesso.")

    try:
        df_bd = pd.read_sql("SELECT * FROM operacoes_estruturadas", con=engine)
    except Exception:
        df_bd = pd.DataFrame(columns=[
            'Conta', 'Cliente', 'Assessor', 'Codigo_da_Operacao', 'Data_Registro', 'Ativo', 'Estrutura',
            'preco_at_
