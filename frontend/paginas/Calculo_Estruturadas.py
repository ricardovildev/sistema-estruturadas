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
        # Aqui você pode usar seu método para atualizar preços atuais na tabela ativos_yahoo
        # Exemplo simplificado (executar procedimento já implementado)
        conn.execute(text("UPDATE ativos_yahoo SET preco_atual = preco_atual"))  # Substitua pela chamada real
    st.success("Preços atualizados com sucesso.")

def calcular_resultados(engine, df):
    hoje = pd.Timestamp.now().normalize()

    # Buscar preco_fechamento da tabela historico_precos baseado em ativo e data de vencimento, só para registros que não tem preco_fechamento ou resultado
    query = text("""
        SELECT hp.codigo_bdi, hp.data_pregao, hp.preco_fechamento
        FROM historico_precos hp
    """)
    df_precos = pd.read_sql(query, engine)

    df = df.copy()
    # Inicializar colunas se não existirem
    for col in ['preco_fechamento', 'resultado']:
        if col not in df.columns:
            df[col] = pd.NA

    atualizacoes = []

    for idx, row in df.iterrows():
        # Não recalcular se já tem preco_fechamento e resultado
        if pd.notna(row.get('preco_fechamento')) and pd.notna(row.get('resultado')):
            continue

        # Filtrar preco_fechamento para o ativo e data vencimento
        ativo = row['Ativo']
        vencimento = row['Data Vencimento']
        precos_ativos = df_precos[
            (df_precos['codigo_bdi'] == ativo) & 
            (df_precos['data_pregao'] == pd.to_datetime(vencimento))
        ]

        if not precos_ativos.empty:
            preco_fech = precos_ativos['preco_fechamento'].iloc[0]
            df.at[idx, 'preco_fechamento'] = preco_fech

            # Calculo exemplo de resultado para financiamento sem barreira (como antes)
            strike = row.get('Valor do Strike (1)', 0)
            qtd = row.get('Quantidade Ativa (1)', 0)
            custo_unit = row.get('Custo Unitário Cliente', 0)
            preco_atual = preco_fech

            # Ajuste e resultado baseado no que você definiu
            ajuste = 0
            resultado = 0
            if preco_atual > strike and qtd < 0:  # Exemplo: call vendida
                ajuste = (strike - preco_atual) * qtd
                resultado = preco_atual * abs(qtd) + ajuste + custo_unit * abs(qtd)
            else:
                resultado = custo_unit * abs(qtd)  # premio recebido

            df.at[idx, 'resultado'] = resultado

            atualizacoes.append({
                'id': row.get('id', None),  # precisa de ID ou chave para update
                'preco_fechamento': preco_fech,
                'resultado': resultado,
            })

    # Atualizar banco com os resultados calculados se tiver coluna id para update
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

            # Salvar df importado no banco (aqui salvar na tabela das operações)
            df.to_sql('suas_tabela_operacoes', con=engine, if_exists='replace', index=False)
            st.success("Planilha importada e salva no banco com sucesso.")

    if st.button("Atualizar preços atuais"):
        atualizar_preco_ativos(engine)

    df_bd = pd.read_sql("SELECT * FROM suas_tabela_operacoes", con=engine)

    if st.button("Calcular Resultados"):
        df_bd = calcular_resultados(engine, df_bd)

    # Ajustar nome coluna para Conta e adicionar Cliente e Assessor ao lado
    df_bd = df_bd.rename(columns={'Código do Cliente': 'Conta'})

    # Exibir tabela com colunas Conta, Cliente, Assessor, etc.
    colunas_para_exibir = ['Conta', 'Código do Assessor', 'Código da Operação', 'Data Registro', 'Ativo', 
                          'Estrutura', 'preco_fechamento', 'resultado', 'Ajuste', 'Status', 'Volume', 'Cupons/Premio']
    colunas_existentes = [c for c in colunas_para_exibir if c in df_bd.columns]

    st.dataframe(df_bd[colunas_existentes])
