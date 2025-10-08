import streamlit as st
import pandas as pd
from backend.conexao import conectar
from sqlalchemy import text
from datetime import datetime
import math

# =========================
# Utilitários
# =========================
def converter_virgula_para_float(df, colunas):
    for col in colunas:
        if col in df.columns:
            df[col] = df[col].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def identificar_opcao(df, seq=1):
    qtd_col = f'Quantidade_Ativa_{seq}'
    tipo_col = f'Tipo_{seq}'
    strike_col = f'Valor_Strike_{seq}'
    if qtd_col in df.columns and tipo_col in df.columns:
        q = pd.to_numeric(df[qtd_col], errors='coerce').fillna(0)
        t = df[tipo_col].fillna('')
        df[f'opcao_call_comprada_{seq}'] = ((q > 0) & (t == 'Call Option')).astype(int)
        df[f'opcao_call_vendida_{seq}'] = ((q < 0) & (t == 'Call Option')).astype(int)
        df[f'opcao_put_comprada_{seq}'] = ((q > 0) & (t == 'Put Option')).astype(int)
        df[f'opcao_put_vendida_{seq}'] = ((q < 0) & (t == 'Put Option')).astype(int)
    if strike_col in df.columns:
        df[f'strike_{seq}'] = pd.to_numeric(df[strike_col], errors='coerce')
    return df

def tratar_quantidade(row):
    try:
        v = row.get('Quantidade_Ativa_1', 0)
        v = float(v) if v not in [None, ''] else 0.0
        if v != 0:
            return abs(v)
        for i in range(1, 5):
            qt_col = f'Quantidade_Ativa_{i}'
            tipo_col = f'Tipo_{i}'
            if qt_col in row and tipo_col in row:
                q = row.get(qt_col, 0)
                t = str(row.get(tipo_col, '')).strip().lower()
                q = float(q) if q not in [None, ''] else 0.0
                if q > 0 and t == 'stock':
                    return abs(q)
    except Exception:
        pass
    return 0.0

def safe_val(val):
    if val is None:
        return None
    if isinstance(val, float) and (math.isnan(val) or val == float('nan')):
        return None
    return val

# =========================
# Atualização de preços (JOIN)
# =========================
def atualizar_preco_ativos(engine):
    with engine.begin() as conn:
        # Vencimentos futuros: preco_atual de ativos_yahoo.asset_original
        conn.execute(text("""
            UPDATE operacoes_estruturadas oe
            JOIN ativos_yahoo ay ON UPPER(TRIM(oe.Ativo)) = UPPER(TRIM(ay.asset_original))
            SET oe.preco_atual = ay.preco_atual
            WHERE oe.Data_Vencimento > CURDATE()
        """))
        # Vencimentos passados/hoje: preco_fechamento de historico_precos (matching data_pregao)
        conn.execute(text("""
            UPDATE operacoes_estruturadas oe
            JOIN historico_precos hp ON UPPER(TRIM(oe.Ativo)) = UPPER(TRIM(hp.codigo_bdi))
            AND oe.Data_Vencimento = hp.data_pregao
            SET oe.preco_fechamento = hp.preco_ultimo
            WHERE oe.Data_Vencimento <= CURDATE()
        """))
    st.success("Preços atualizados conforme a data de vencimento.")

# =========================
# Cálculo de resultados
# =========================
def calcular_resultados(engine, df):
    hoje = datetime.today().date()
    df = df.copy()
    atualizacoes = []

    for idx, row in df.iterrows():
        estrutura_raw = row.get('Estrutura', '')
        estrutura = str(estrutura_raw).strip().upper()

        vencimento_raw = row.get('Data_Vencimento')
        if pd.isna(vencimento_raw):
            continue
        try:
            vencimento = pd.to_datetime(vencimento_raw).date()
        except Exception:
            continue

        # Preço de referência conforme vencimento
        preco = row.get('preco_atual') if vencimento > hoje else row.get('preco_fechamento')
        if preco in [None, ''] or pd.isna(preco):
            continue
        try:
            preco = float(preco)
        except Exception:
            continue

        # Coerção de entradas numéricas
        quantidade = pd.to_numeric(row.get('Quantidade', 0), errors='coerce')
        valor_ativo = pd.to_numeric(row.get('Valor_Ativo', 0), errors='coerce')
        custo_unit = pd.to_numeric(row.get('Custo_Unitario_Cliente', 0), errors='coerce')
        strike_call_vendida = pd.to_numeric(row.get('Valor_Strike_1', 0), errors='coerce')
        dividendos = pd.to_numeric(row.get('dividendos', 0), errors='coerce')

        for name in ['quantidade','valor_ativo','custo_unit','strike_call_vendida','dividendos']:
            if locals()[name] is None or pd.isna(locals()[name]):
                locals()[name] = 0.0

        quantidade = float(quantidade)
        valor_ativo = float(valor_ativo)
        custo_unit = float(custo_unit)
        strike_call_vendida = float(strike_call_vendida)
        dividendos = float(dividendos)

        # Campos calculados
        cupons_premio = quantidade * custo_unit if (quantidade and custo_unit) else 0.0
        ajuste = None
        resultado = None
        status = None

        # Apenas FINANCIAMENTO aplica a lógica
        if 'FINANCIAMENTO' in estrutura:
            if preco > strike_call_vendida:
                # Exerce: há ajuste
                ajuste = (strike_call_vendida - preco) * quantidade
                resultado = (preco - valor_ativo + dividendos) * quantidade + ajuste + cupons_premio
                status = "Ação Vendida"
            else:
                # Virou pó (<= strike)
                ajuste = 0.0
                resultado = cupons_premio
                status = "Virando Pó" if vencimento > hoje else "Virou Pó"

        # Volume, investido, percentual (apenas se houver resultado calculado)
        volume = quantidade * preco if (quantidade and preco) else None
        investido = pd.to_numeric(row.get('investido', quantidade * valor_ativo), errors='coerce')
        investido = None if (investido is None or pd.isna(investido)) else float(investido)
        percentual = (resultado / investido) if (resultado is not None and investido and investido != 0) else None

        # Montar dicionário somente com campos a atualizar (evita sobrescrever com None indevido)
        update_dict = {'id': row.get('id', None)}
        if resultado is not None:
            update_dict['resultado'] = safe_val(resultado)
        if ajuste is not None:
            update_dict['Ajuste'] = safe_val(ajuste)
        if status is not None:
            update_dict['Status'] = status
        if volume is not None:
            update_dict['Volume'] = safe_val(volume)
        # Cupons_Premio só atualiza se for financiamento (mantém coerência com regra)
        if 'FINANCIAMENTO' in estrutura:
            update_dict['Cupons_Premio'] = safe_val(cupons_premio)
        if percentual is not None:
            update_dict['percentual'] = safe_val(percentual)

        # Só inclui se houver id e ao menos um campo (além do id) para atualizar
        if update_dict.get('id') is not None and len(update_dict.keys()) > 1:
            atualizacoes.append(update_dict)

    # Persistência: atualiza apenas campos presentes no dicionário
    with engine.begin() as conn:
        for a in atualizacoes:
            sets = []
            params = {}
            for k, v in a.items():
                if k == 'id':
                    continue
                sets.append(f"{k} = :{k}")
                params[k] = v
            params['id'] = a['id']
            if sets:
                sql = f"UPDATE operacoes_estruturadas SET {', '.join(sets)} WHERE id = :id"
                conn.execute(text(sql), params)

    st.success(f"Foram atualizados {len(atualizacoes)} registros.")
    return df



# =========================
# UI
# =========================
def render():
    st.title("Cálculo de Resultados das Operações Estruturadas")
    engine = conectar()

    st.write("### Importar Planilha Relatório de Posição (1)")
    arquivo = st.file_uploader("Escolha o arquivo (.xlsx)", type=["xlsx"])

    if arquivo:
        if st.button("Importar Planilha"):
            df = pd.read_excel(arquivo, dtype=str)

            # 1) Converter numéricos com vírgula
            colunas_numericas_planilha = [
                'Valor Ativo', 'Custo Unitário Cliente', 'Comissão Assessor',
                'Quantidade Ativa (1)', 'Quantidade Boleta (1)', '% do Strike (1)', 'Valor do Strike (1)',
                '% da Barreira (1)', 'Valor da Barreira (1)', 'Valor do Rebate (1)',
                'Quantidade Ativa (2)', 'Quantidade Boleta (2)', '% do Strike (2)', 'Valor do Strike (2)',
                '% da Barreira (2)', 'Valor da Barreira (2)', 'Valor do Rebate (2)',
                'Quantidade Ativa (3)', 'Quantidade Boleta (3)', '% do Strike (3)', 'Valor do Strike (3)',
                '% da Barreira (3)', 'Valor da Barreira (3)', 'Valor do Rebate (3)',
                'Quantidade Ativa (4)', 'Quantidade Boleta (4)', '% do Strike (4)', 'Valor do Strike (4)',
                '% da Barreira (4)', 'Valor da Barreira (4)', 'Valor do Rebate (4)',
                'Dividendos'
            ]
            df = converter_virgula_para_float(df, [c for c in colunas_numericas_planilha if c in df.columns])

            # 2) Datas
            df['Data Registro'] = pd.to_datetime(df.get('Data Registro'), dayfirst=True, errors='coerce')
            df['Data Vencimento'] = pd.to_datetime(df.get('Data Vencimento'), dayfirst=True, errors='coerce')

            # 3) Renomear para colunas do banco
            renomear = {
                'Código do Cliente': 'Conta',
                'Cliente': 'Cliente',
                'Código do Assessor': 'Assessor',
                'Código da Operação': 'Codigo_da_Operacao',
                'Data Registro': 'Data_Registro',
                'Data Vencimento': 'Data_Vencimento',
                'Ativo': 'Ativo',
                'Estrutura': 'Estrutura',
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
                'Dividendos': 'dividendos',
            }
            df = df.rename(columns=renomear)

            # 4) Remover colunas que não existem no banco (ex.: Canal de Origem)
            colunas_validas = set([
                'Conta','Cliente','Assessor','Codigo_da_Operacao','Data_Registro','Ativo','Estrutura','Valor_Ativo',
                'Data_Vencimento','Custo_Unitario_Cliente','Comissao_Assessor',
                'Quantidade_Ativa_1','Quantidade_Boleta_1','Tipo_1','Percentual_Strike_1','Valor_Strike_1','Percentual_Barreira_1','Valor_Barreira_1','Valor_Rebate_1','Tipo_Barreira_1',
                'Quantidade_Ativa_2','Quantidade_Boleta_2','Tipo_2','Percentual_Strike_2','Valor_Strike_2','Percentual_Barreira_2','Valor_Barreira_2','Valor_Rebate_2','Tipo_Barreira_2',
                'Quantidade_Ativa_3','Quantidade_Boleta_3','Tipo_3','Percentual_Strike_3','Valor_Strike_3','Percentual_Barreira_3','Valor_Barreira_3','Valor_Rebate_3','Tipo_Barreira_3',
                'Quantidade_Ativa_4','Quantidade_Boleta_4','Tipo_4','Percentual_Strike_4','Valor_Strike_4','Percentual_Barreira_4','Valor_Barreira_4','Valor_Rebate_4','Tipo_Barreira_4',
                'dividendos'
            ])
            df = df[[c for c in df.columns if c in colunas_validas]].copy()

            # 5) Tipos e quantidade
            for c in ['Valor_Ativo','Custo_Unitario_Cliente','Comissao_Assessor',
                      'Percentual_Strike_1','Valor_Strike_1','Percentual_Barreira_1','Valor_Barreira_1','Valor_Rebate_1',
                      'Percentual_Strike_2','Valor_Strike_2','Percentual_Barreira_2','Valor_Barreira_2','Valor_Rebate_2',
                      'Percentual_Strike_3','Valor_Strike_3','Percentual_Barreira_3','Valor_Barreira_3','Valor_Rebate_3',
                      'Percentual_Strike_4','Valor_Strike_4','Percentual_Barreira_4','Valor_Barreira_4','Valor_Rebate_4',
                      'dividendos',
                      'Quantidade_Ativa_1','Quantidade_Boleta_1','Quantidade_Ativa_2','Quantidade_Boleta_2',
                      'Quantidade_Ativa_3','Quantidade_Boleta_3','Quantidade_Ativa_4','Quantidade_Boleta_4']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')

            df['Quantidade'] = df.apply(tratar_quantidade, axis=1).astype(float)
            df['investido'] = (pd.to_numeric(df.get('Valor_Ativo', 0), errors='coerce').fillna(0)
                               * pd.to_numeric(df.get('Quantidade', 0), errors='coerce').fillna(0))

            # 6) Flags e strikes
            for i in range(1, 5):
                df = identificar_opcao(df, i)

            # 7) Preços e campos de cálculo padrão
            if 'preco_atual' not in df.columns:
                df['preco_atual'] = None
            if 'preco_fechamento' not in df.columns:
                df['preco_fechamento'] = None
            for base in ['resultado','Ajuste','Status','Volume','Cupons_Premio','percentual']:
                if base not in df.columns:
                    df[base] = None

            # 8) Seleção final conforme schema do banco
            colunas_final = [
                'Conta','Cliente','Assessor','Codigo_da_Operacao','Data_Registro','Ativo','Estrutura','Valor_Ativo',
                'Data_Vencimento','Custo_Unitario_Cliente','Comissao_Assessor',
                'Quantidade_Ativa_1','Quantidade_Boleta_1','Tipo_1','Percentual_Strike_1','Valor_Strike_1','Percentual_Barreira_1','Valor_Barreira_1','Valor_Rebate_1','Tipo_Barreira_1',
                'Quantidade_Ativa_2','Quantidade_Boleta_2','Tipo_2','Percentual_Strike_2','Valor_Strike_2','Percentual_Barreira_2','Valor_Barreira_2','Valor_Rebate_2','Tipo_Barreira_2',
                'Quantidade_Ativa_3','Quantidade_Boleta_3','Tipo_3','Percentual_Strike_3','Valor_Strike_3','Percentual_Barreira_3','Valor_Barreira_3','Valor_Rebate_3','Tipo_Barreira_3',
                'Quantidade_Ativa_4','Quantidade_Boleta_4','Tipo_4','Percentual_Strike_4','Valor_Strike_4','Percentual_Barreira_4','Valor_Barreira_4','Valor_Rebate_4','Tipo_Barreira_4',
                'Quantidade',
                'opcao_call_comprada_1','opcao_call_vendida_1','opcao_put_comprada_1','opcao_put_vendida_1','strike_1',
                'opcao_call_comprada_2','opcao_call_vendida_2','opcao_put_comprada_2','opcao_put_vendida_2','strike_2',
                'opcao_call_comprada_3','opcao_call_vendida_3','opcao_put_comprada_3','opcao_put_vendida_3','strike_3',
                'opcao_call_comprada_4','opcao_call_vendida_4','opcao_put_comprada_4','opcao_put_vendida_4','strike_4',
                'preco_atual','preco_fechamento','resultado','Ajuste','Status','Volume','Cupons_Premio',
                'dividendos','investido','percentual'
            ]
            df = df[[c for c in colunas_final if c in df.columns]].copy()

            # 9) Evitar NaN no INSERT
            for c in ['Valor_Ativo','Custo_Unitario_Cliente','Comissao_Assessor',
                      'Percentual_Strike_1','Valor_Strike_1','Percentual_Barreira_1','Valor_Barreira_1','Valor_Rebate_1',
                      'Percentual_Strike_2','Valor_Strike_2','Percentual_Barreira_2','Valor_Barreira_2','Valor_Rebate_2',
                      'Percentual_Strike_3','Valor_Strike_3','Percentual_Barreira_3','Valor_Barreira_3','Valor_Rebate_3',
                      'Percentual_Strike_4','Valor_Strike_4','Percentual_Barreira_4','Valor_Barreira_4','Valor_Rebate_4',
                      'Quantidade','strike_1','strike_2','strike_3','strike_4','preco_atual','preco_fechamento',
                      'resultado','Ajuste','Volume','Cupons_Premio','dividendos','investido','percentual']:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors='coerce')

            for c in ['opcao_call_comprada_1','opcao_call_vendida_1','opcao_put_comprada_1','opcao_put_vendida_1',
                      'opcao_call_comprada_2','opcao_call_vendida_2','opcao_put_comprada_2','opcao_put_vendida_2',
                      'opcao_call_comprada_3','opcao_call_vendida_3','opcao_put_comprada_3','opcao_put_vendida_3',
                      'opcao_call_comprada_4','opcao_call_vendida_4','opcao_put_comprada_4','opcao_put_vendida_4']:
                if c in df.columns:
                    df[c] = df[c].fillna(0).astype(int)

            for c in ['Data_Registro','Data_Vencimento']:
                if c in df.columns:
                    df[c] = pd.to_datetime(df[c], errors='coerce')

            df = df.where(pd.notnull(df), None)

            df.to_sql('operacoes_estruturadas', con=engine, if_exists='append', index=False)
            st.success("Planilha importada e inserida no banco com sucesso.")
            st.rerun()

    # Carregar dados
    try:
        df_bd = pd.read_sql("SELECT * FROM operacoes_estruturadas", con=engine)
    except Exception:
        df_bd = pd.DataFrame(columns=[
            'Conta','Cliente','Assessor','Codigo_da_Operacao','Data_Registro','Ativo','Estrutura',
            'preco_atual','preco_fechamento','resultado','Ajuste','Status','Volume','Cupons_Premio','percentual','dividendos','investido'
        ])

    # Filtros
    st.write("### Filtros para Consulta")
    with st.form("form_filtros"):
        filtro_conta = st.multiselect('Conta', options=df_bd['Conta'].dropna().unique() if 'Conta' in df_bd else [])
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
        'Conta','Cliente','Assessor','Codigo_da_Operacao','Data_Registro','Ativo','Estrutura',
        'preco_atual','preco_fechamento','resultado','Ajuste','Status','Volume','Cupons_Premio','percentual','dividendos','investido'
    ]
    colunas_existentes = [c for c in colunas_para_exibir if c in df_filtrado.columns]
    st.dataframe(df_filtrado[colunas_existentes])

    # Ações
    if st.button("Atualizar Preços"):
        atualizar_preco_ativos(engine)
        st.rerun()

    if st.button("Calcular Resultados"):
        df_bd = calcular_resultados(engine, df_bd)
        st.rerun()
