import pandas as pd
import numpy as np
import re
import sys
import os
import yfinance as yf
from datetime import datetime
from datetime import date
from sqlalchemy import text
from io import StringIO
import bcrypt
import streamlit as st


# Adiciona a pasta raiz (Estruturadas) ao path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from backend.conexao import conectar

engine = conectar()  # cria a conexão uma vez

def importar_ativos_yahoo(caminho_arquivo: str, tipo_arquivo: str = 'csv') -> str:
    """
    Importa ativos para a tabela ativos_yahoo sem apagar colunas extras e sem duplicar registros.
    """
    try:
        if not os.path.exists(caminho_arquivo):
            return f"Arquivo não encontrado em: {caminho_arquivo}"

        engine = conectar()

        # Leitura do arquivo
        if tipo_arquivo == 'csv':
            df = pd.read_csv(caminho_arquivo)
        elif tipo_arquivo == 'excel':
            df = pd.read_excel(caminho_arquivo)
        else:
            return "Tipo de arquivo inválido. Use 'csv' ou 'excel'."

        # Verificação das colunas obrigatórias
        if not {'asset_original', 'asset_yahoo'}.issubset(df.columns):
            return "O arquivo deve conter as colunas 'asset_original' e 'asset_yahoo'."
        
        # Se asset_yahoo não existir, cria com sufixo .SA
        if 'asset_yahoo' not in df.columns:
            df['asset_yahoo'] = df['asset_original'].astype(str).str.strip() + '.SA'
        else:
        # Preenche valores ausentes com sufixo .SA
            df['asset_yahoo'] = df['asset_yahoo'].fillna(df['asset_original'].astype(str).str.strip() + '.SA')

        # Remover linhas com dados ausentes
        df = df.dropna(subset=['asset_original', 'asset_yahoo'])

        # Inserção ou atualização linha a linha
        with engine.begin() as conn:
            for _, row in df.iterrows():
                query = text("""
                    INSERT INTO ativos_yahoo (asset_original, asset_yahoo)
                    VALUES (:original, :yahoo)
                    ON DUPLICATE KEY UPDATE asset_yahoo = VALUES(asset_yahoo)
                """)
                conn.execute(query, {
                    "original": str(row['asset_original']).strip(),
                    "yahoo": str(row['asset_yahoo']).strip()
                })

        return "Ativos importados com sucesso, sem duplicações e mantendo colunas extras."

    except Exception as e:
        return f"Erro ao importar ativos: {str(e)}"


def obter_lista_assets(engine):
    query = "SELECT asset_original FROM ativos_yahoo"
    df = pd.read_sql(query, engine)
    return df


def obter_preco_ultimo(asset):
    try:
        ticker_formatado = asset.strip().upper()
        ativo = yf.Ticker(ticker_formatado)
        historico = ativo.history(period="1d")
        
        if historico.empty:
            return None
        
        preco = historico["Close"].iloc[-1]
        return round(preco, 2)
    except Exception as e:
        print(f"Erro ao obter preço de {asset}: {e}")
        return None
    
def atualizar_preco(engine, ticker, preco):
    hoje = date.today()
    query = text("""
        UPDATE ativos_yahoo
        SET preco_atual = :preco,
            data_atualizacao = :data
        WHERE asset_original = :ticker
    """)
    
    try:
        with engine.begin() as conn:
            conn.execute(query, {"preco": preco, "data": hoje, "ticker": ticker})
    except Exception as e:
        print(f"Erro ao atualizar {ticker}: {e}")


def importar_clientes(caminho_excel):
    df = pd.read_excel(caminho_excel)
    engine = conectar()
    df.to_sql('clientes', con=engine, if_exists='append', index=False)
    print("Clientes importados com sucesso.")

def importar_vencimentos_opcoes(caminho_excel):
    df = pd.read_excel(caminho_excel)
    df['data_vencimento'] = pd.to_datetime(df['data_vencimento'], dayfirst=True, errors='coerce').dt.date
    engine = conectar()
    df.to_sql('vencimentos_opcoes', con=engine, if_exists='append', index=False)
    print("Vencimentos de opções importados com sucesso.")

def importar_historico_precos():
    import pandas as pd
    from io import StringIO
    from backend.conexao import conectar

    # === CONFIGURAÇÕES ===
    caminho_arquivo = r'D:/Backup 30-08-2025/Documentos/Sistema Estruturadas/Historico de precos B3/COTAHIST_A2025.txt'
    nome_tabela = 'historico_precos'

    # === EXTRAÇÃO E TRANSFORMAÇÃO ===
    colspecs = [
        (2, 10),   # data_pregao
        (12, 24),  # codigo_bdi
        (24, 36),  # codigo_negociacao
        (27, 39),  # nome_empresa
        (39, 49),  # especificacao_papel
        (56, 69),  # preco_abertura
        (69, 82),  # preco_maximo
        (82, 95),  # preco_minimo
        (95, 108), # preco_medio
        (108, 121),# preco_fechamento
        (152, 170) # volume_negociado
    ]

    colnames = [
        'data_pregao', 'codigo_bdi', 'codigo_negociacao', 'nome_empresa',
        'especificacao_papel', 'preco_abertura', 'preco_maximo',
        'preco_minimo', 'preco_medio', 'preco_fechamento', 'volume'
    ]

    # Ler todas as linhas do arquivo
    with open(caminho_arquivo, 'r', encoding='latin1') as f:
        linhas = f.readlines()

    linhas_validas = [linha for linha in linhas if linha.startswith('01')]

    df = pd.read_fwf(StringIO(''.join(linhas_validas)), colspecs=colspecs, names=colnames)

    # Tratar colunas numéricas e datas
    df['data_pregao'] = pd.to_datetime(df['data_pregao'], format='%Y%m%d')
    colunas_preco = ['preco_abertura', 'preco_maximo', 'preco_minimo', 'preco_medio', 'preco_fechamento']
    df[colunas_preco] = df[colunas_preco].astype(float) / 100
    df['volume'] = df['volume'].astype(float) / 100

    # === CARGA PARA O BANCO ===
    engine = conectar()

    # Buscar registros já existentes
    df_existente = pd.read_sql(f"SELECT data_pregao, codigo_bdi FROM {nome_tabela}", engine)

    # Garantir mesmo tipo
    df['data_pregao'] = pd.to_datetime(df['data_pregao'])
    df_existente['data_pregao'] = pd.to_datetime(df_existente['data_pregao'])

    # Filtrar apenas registros novos
    df_novo = df.merge(df_existente, on=['data_pregao', 'codigo_bdi'], how='left', indicator=True)
    df_novo = df_novo[df_novo['_merge'] == 'left_only'].drop(columns=['_merge'])

    if not df_novo.empty:
        df_novo.to_sql(nome_tabela, con=engine, if_exists='append', index=False)
        print(f"{len(df_novo)} registros novos importados para a tabela '{nome_tabela}'.")
    else:
        print("Nenhum registro novo para importar.")



def importar_ativos(caminho_excel):
    df = pd.read_excel(caminho_excel)
    df['Data_negociacao'] = pd.to_datetime(df['Data_negociacao'], dayfirst=True).dt.date
    df['Vencimento'] = pd.to_datetime(df['Vencimento'], errors='coerce', format='%d/%m/%Y').dt.date
    engine = conectar()
    df.to_sql('ativos', con=engine, if_exists='append', index=False)
    print("Ativos importados com sucesso.")

def importar_notas_atualizado(caminho_excel_notas='D:/Backup 30-08-2025/Documentos/Meus_Projetos/Notas.xlsx'):
    
    
    # Leitura da planilha
    df = pd.read_excel(caminho_excel_notas, decimal=',')
    df['data_registro'] = pd.to_datetime(df['data_registro'], dayfirst=True).dt.date

    # Inserir novos dados
    tabela_destino = 'notas'
    df.to_sql(tabela_destino, con=engine, if_exists='append', index=False)

    # Carregar dados existentes da tabela para atualização
    query = text("""
        SELECT id, tipo_mercado, especificacao, on_pn_strike, ativo_base, vencimento 
        FROM notas 
        WHERE tipo_mercado LIKE 'OPCAO%' 
           OR tipo_mercado IN ('EXERC OPC VENDA', 'EXERC OPC COMPRA', 'A VISTA', 'VISTA','FRACIONARIO')
    """)
    df_notas = pd.read_sql(query, engine)

    # Define tipo_papel com base em tipo_mercado
    def definir_tipo_papel(tipo_mercado):
        tipo = tipo_mercado.upper().strip()
        if tipo in ['OPCAO DE VENDA', 'OPCAO DE COMPRA']:
            return 'OPCAO'
        elif tipo in ['EXERC OPC VENDA', 'EXERC OPC COMPRA', 'A VISTA','VISTA','FRACIONARIO']:
            return 'ACAO'
        else:
            return 'ACAO'

    df_notas['tipo_papel'] = df_notas['tipo_mercado'].apply(definir_tipo_papel)

    # Define tipo_opcao com base em tipo_mercado
    df_notas['tipo_opcao'] = df_notas['tipo_mercado'].apply(
        lambda x: 'CALL' if 'COMPRA' in x.upper() else ('PUT' if 'VENDA' in x.upper() else None)
    )

    # Extrair strike da coluna on_pn_strike
    def extrair_strike(valor):
        try:
            if not isinstance(valor, str):
                return None
            valor_limpo = valor.strip().replace(',', '.')
            match = re.search(r'(\d+\.\d+)', valor_limpo)
            return float(match.group(1)) if match else None
        except:
            return None

    df_notas['strike'] = df_notas['especificacao'].apply(extrair_strike)

    df_notas['on_pn_strike'] = None  # para deixar vazia

    # Substituir NaN por None
    df_notas = df_notas.where(pd.notnull(df_notas), None)
    df_notas['strike'] = df_notas['strike'].apply(lambda x: None if pd.isna(x) else x)

    # Extrair letra da opção
    df_notas['letra_call_put'] = df_notas['especificacao'].str[4]

    # Carregar vencimentos
    df_vencimentos = pd.read_sql("SELECT codigo_letra, data_vencimento FROM vencimentos_opcoes", engine)
    df_datas_registro = pd.read_sql("SELECT id, data_registro FROM notas", engine)
    df_notas = df_notas.merge(df_datas_registro, on='id', how='left')

    # Função para encontrar vencimento
    def encontrar_vencimento(letra, data_registro):
        datas_possiveis = df_vencimentos[
            (df_vencimentos['codigo_letra'] == letra) &
            (df_vencimentos['data_vencimento'] > data_registro)
        ].sort_values('data_vencimento')
        return datas_possiveis.iloc[0]['data_vencimento'] if not datas_possiveis.empty else None

    df_notas['vencimento'] = df_notas.apply(
        lambda row: encontrar_vencimento(row['letra_call_put'], row['data_registro']),
        axis=1
    )

    # Atualizar os dados no banco
    with engine.begin() as conn:
        for _, row in df_notas.iterrows():
            conn.execute(text("""
                UPDATE notas
                SET tipo_papel = :tipo_papel,
                    tipo_opcao = :tipo_opcao,
                    strike = :strike,
                    ativo_base = :ativo_base,
                    letra_call_put = :letra_call_put,
                    vencimento = :vencimento
                WHERE id = :id
            """), {
                'tipo_papel': row['tipo_papel'],
                'tipo_opcao': row['tipo_opcao'] if pd.notna(row['tipo_opcao']) else None,
                'strike': row['strike'] if pd.notna(row['strike']) else None,
                'ativo_base': row['ativo_base'] if pd.notna(row['ativo_base']) else None,
                'letra_call_put': row['letra_call_put'] if pd.notna(row['letra_call_put']) else None,
                'vencimento': row['vencimento'] if pd.notna(row['vencimento']) else None,
                'id': row['id']
            })

    print("✅ Dados atualizados com sucesso na tabela 'notas'")
    print(f"📥 Dados importados com sucesso para a tabela '{tabela_destino}'")


def calcular_resultado_opcoes():
    engine = conectar()
    hoje = pd.Timestamp(datetime.today().date())

    query = """
    SELECT 
        n.id, n.ativo_base, n.tipo_opcao, n.strike, n.vencimento, n.data_registro,
        h.preco_fechamento, COALESCE(SUM(p.valor), 0) AS total_proventos
    FROM notas n
    LEFT JOIN (
        SELECT codigo_bdi, MAX(data_pregao) AS data_mais_recente
        FROM historico_precos
        GROUP BY codigo_bdi
    ) ult ON ult.codigo_bdi = n.ativo_base
    LEFT JOIN historico_precos h ON h.codigo_bdi = n.ativo_base
        AND (
            (n.vencimento < %(hoje)s AND h.data_pregao = n.vencimento)
            OR (n.vencimento >= %(hoje)s AND h.data_pregao = ult.data_mais_recente)
        )
    LEFT JOIN proventos p ON p.ativo = n.ativo_base
        AND p.data_com > n.data_registro AND p.data_com <= n.vencimento
    WHERE n.tipo_mercado LIKE 'OPCAO%%'
    GROUP BY n.id, n.ativo_base, n.tipo_opcao, n.strike, n.vencimento, n.data_registro, h.preco_fechamento
    """

    df = pd.read_sql(query, engine, params={"hoje": hoje})
    # Padronizar datas para evitar erro de comparação
    df['vencimento'] = pd.to_datetime(df['vencimento'])
    df['data_registro'] = pd.to_datetime(df['data_registro'])

    df['strike_ajustado'] = df['strike'] - df['total_proventos']

    def calcular_resultado(row):
        vencida = row['vencimento'] < hoje
        tipo = row['tipo_opcao']
        preco = row['preco_fechamento']
        strike = row['strike_ajustado']
        if pd.isna(preco) or pd.isna(strike): return None

        if vencida:
            if tipo == 'CALL':
                return 'Exercicio' if preco >= strike else 'Virou Pó'
            elif tipo == 'PUT':
                return 'Exercicio' if preco <= strike else 'Virou Pó'
        else:
            if tipo == 'CALL':
                return 'Indo a Exercicio' if preco >= strike else 'Virando Pó'
            elif tipo == 'PUT':
                return 'Indo a Exercicio' if preco <= strike else 'Virando Pó'

    df['resultado'] = df.apply(calcular_resultado, axis=1)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            if row['resultado']:
                conn.execute(
                    text("UPDATE notas SET resultado = :resultado WHERE id = :id"),
                    {"resultado": row["resultado"], "id": row["id"]}
                )

    print("Resultados das opções atualizados com sucesso.")

   
def importar_proventos(caminho_excel):
    df = pd.read_excel(caminho_excel)

    # Validação de colunas
    colunas_esperadas = ['ativo', 'tipo', 'data_com', 'data_pagamento', 'valor']
    if not all(col in df.columns for col in colunas_esperadas):
        raise ValueError("A planilha não contém todas as colunas esperadas.")

    # Conversão de tipos
    df['data_com'] = pd.to_datetime(df['data_com'], dayfirst=True, errors='coerce').dt.date
    df['data_pagamento'] = pd.to_datetime(df['data_pagamento'], dayfirst=True, errors='coerce').dt.date
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce')

    # Remover linhas inválidas
    df = df.dropna(subset=['ativo', 'tipo', 'data_com', 'valor'])

    # Conexão e inserção
    engine = conectar()
    df.to_sql('proventos', con=engine, if_exists='append', index=False)

    print("✅ Proventos importados com sucesso.")

def atualizar_historico_operacoes():
    import pandas as pd
    from sqlalchemy import text
    from backend.conexao import conectar

    engine = conectar()

    # ------------------------------
    # Leitura das tabelas
    # ------------------------------
    notas = pd.read_sql("SELECT * FROM notas", engine)
    precos = pd.read_sql("SELECT * FROM historico_precos", engine)
    prov = pd.read_sql("SELECT * FROM proventos", engine)

    notas['data_registro'] = pd.to_datetime(notas['data_registro'], errors='coerce')
    precos['data_pregao'] = pd.to_datetime(precos['data_pregao'], errors='coerce')
    prov['data_com'] = pd.to_datetime(prov['data_com'], errors='coerce')

    # ------------------------------
    # Limpar histórico anterior
    # ------------------------------
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM historico_operacoes"))

    # ------------------------------
    # Separar ações e opções
    # ------------------------------
    acoes = notas[notas['tipo_papel'] == 'ACAO']
    opcoes = notas[notas['tipo_papel'] == 'OPCAO']

    # ------------------------------
    # Quantidades e valores
    # ------------------------------
    compras = acoes[acoes['tipo_lado'] == 'C'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Quantidade_comprada=('quantidade', 'sum'),
        Total_compras=('valor_operacao', 'sum'),
        data_inicio=('data_registro', 'min')
    ).reset_index()

    # Preço médio
    def safe_div(num, den):
        return num / den if den != 0 else 0

    compras['preco_medio'] = compras.apply(
        lambda x: safe_div(x['Total_compras'], x['Quantidade_comprada']), axis=1
    )

    vendas = acoes[acoes['tipo_lado'] == 'V'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Quantidade_vendida=('quantidade', 'sum'),
        Total_vendas=('valor_operacao', 'sum')
    ).reset_index()

    # Garantir data mínima para cada ativo
    datas_todos_ativos = notas.groupby(['conta', 'cliente', 'ativo_base'])['data_registro'].min().reset_index()
    datas_todos_ativos.rename(columns={'data_registro': 'data_inicio'}, inplace=True)

    # ------------------------------
    # Prêmios de opções
    # ------------------------------
    premios_recebidos = opcoes[opcoes['tipo_lado'] == 'V'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Premios_recebidos=('valor_operacao', 'sum')
    ).reset_index()

    premios_pagos = opcoes[opcoes['tipo_lado'] == 'C'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Premios_pagos=('valor_operacao', 'sum')
    ).reset_index()

    # ------------------------------
    # Consolidar dados
    # ------------------------------
    consolidado = pd.merge(datas_todos_ativos, compras, on=['conta', 'cliente', 'ativo_base'], how='left')
    consolidado = pd.merge(consolidado, vendas, on=['conta', 'cliente', 'ativo_base'], how='left')
    consolidado = pd.merge(consolidado, premios_recebidos, on=['conta', 'cliente', 'ativo_base'], how='left')
    consolidado = pd.merge(consolidado, premios_pagos, on=['conta', 'cliente', 'ativo_base'], how='left')

    # Corrigir colunas de data_inicio
    if 'data_inicio_x' in consolidado.columns and 'data_inicio_y' in consolidado.columns:
        consolidado['data_inicio'] = consolidado[['data_inicio_x', 'data_inicio_y']].min(axis=1)
        consolidado.drop(columns=['data_inicio_x', 'data_inicio_y'], inplace=True)
    elif 'data_inicio_x' in consolidado.columns:
        consolidado.rename(columns={'data_inicio_x': 'data_inicio'}, inplace=True)
    elif 'data_inicio_y' in consolidado.columns:
        consolidado.rename(columns={'data_inicio_y': 'data_inicio'}, inplace=True)

    consolidado.fillna({
        'Quantidade_comprada': 0,
        'Total_compras': 0,
        'Quantidade_vendida': 0,
        'Total_vendas': 0,
        'Premios_recebidos': 0,
        'Premios_pagos': 0,
        'preco_medio': 0
    }, inplace=True)

    # Quantidade e prêmios
    consolidado['quantidade_atual'] = consolidado['Quantidade_comprada'] - consolidado['Quantidade_vendida']
    consolidado['Premio_liquido'] = consolidado['Premios_recebidos'] - consolidado['Premios_pagos']

    # ------------------------------
    # Proventos
    # ------------------------------
    proventos_total = []
    for _, row in consolidado.iterrows():
        prov_ativo = prov[prov['ativo'] == row['ativo_base']]
        total_provento = 0
        for _, p in prov_ativo.iterrows():
            qtd_ate_data = acoes[
                (acoes['conta'] == row['conta']) &
                (acoes['cliente'] == row['cliente']) &
                (acoes['ativo_base'] == row['ativo_base']) &
                (acoes['data_registro'] <= p['data_com'])
            ]['quantidade'].sum()
            total_provento += qtd_ate_data * p['valor']
        proventos_total.append(total_provento)

    consolidado['Proventos'] = proventos_total

    # ------------------------------
    # Preços
    # ------------------------------
    ativos_yahoo = pd.read_sql("SELECT DISTINCT asset_original, preco_atual FROM ativos_yahoo", engine)
    precos = precos.drop_duplicates(subset=['codigo_bdi','data_pregao'])

    consolidado = pd.merge(
        consolidado,
        ativos_yahoo[['asset_original', 'preco_atual']],
        left_on='ativo_base',
        right_on='asset_original',
        how='left'
    )

    consolidado['preco_fechamento'] = consolidado['preco_atual'].fillna(0)
    consolidado.drop(columns=['asset_original', 'preco_atual'], inplace=True)

    preco_inicio = pd.merge(
        notas.groupby('ativo_base')['data_registro'].min().reset_index(),
        precos, left_on=['ativo_base', 'data_registro'], right_on=['codigo_bdi', 'data_pregao'], how='left'
    )[['ativo_base', 'preco_fechamento']].rename(columns={'preco_fechamento': 'preco_fechamento_inicio_operacoes'})

    preco_inicio = preco_inicio.drop_duplicates(subset=['ativo_base'])

    consolidado = pd.merge(consolidado, preco_inicio, on='ativo_base', how='left')
    consolidado.rename(columns={'preco_fechamento': 'preco_fechamento'}, inplace=True)

    # ------------------------------
    # Cálculos finais e rentabilidades
    # ------------------------------
    consolidado['Posicao_atual'] = consolidado['quantidade_atual'] * consolidado['preco_fechamento']
    consolidado['investido'] = consolidado['quantidade_atual'] * consolidado['preco_medio']

    consolidado['resultado_sem_opcoes'] = consolidado['Posicao_atual'] - consolidado['investido']
    consolidado['resultado_com_opcoes'] = consolidado['resultado_sem_opcoes'] + consolidado['Premio_liquido']

    consolidado['Rentabilidade_sem_premio'] = consolidado.apply(
        lambda x: safe_div(x['Total_vendas'] + x['Posicao_atual'] - x['Total_compras'], x['Total_compras']), axis=1)
    consolidado['Rentabilidade_com_premio'] = consolidado.apply(
        lambda x: safe_div(x['Total_vendas'] + x['Posicao_atual'] + x['Premio_liquido'] - x['Total_compras'], x['Total_compras']), axis=1)
    consolidado['Rentabilidade_com_proventos'] = consolidado.apply(
        lambda x: safe_div(x['Total_vendas'] + x['Posicao_atual'] + x['Proventos'] - x['Total_compras'], x['Total_compras']), axis=1)
    consolidado['Rentabilidade_com_proventos_premios'] = consolidado.apply(
        lambda x: safe_div(x['Total_vendas'] + x['Posicao_atual'] + x['Proventos'] + x['Premio_liquido'] - x['Total_compras'], x['Total_compras']), axis=1)
    
        # Preço médio de venda (Total Vendas / Quantidade Vendida)
    consolidado['preco_medio_vendas'] = consolidado.apply(
        lambda x: safe_div(x['Total_vendas'], x['Quantidade_vendida']), axis=1)

    # Rentabilidade em reais (valor absoluto)

    # Sem opção: (Preço médio de venda - Preço médio de compra) * Quantidade vendida
    consolidado['rentabilidade_venda_sem_premio'] = (consolidado['preco_medio_vendas'] - consolidado['preco_medio']) * consolidado['Quantidade_vendida']

    # Com prêmio: rentabilidade sem prêmio + prêmios recebidos (em reais)
    consolidado['rentabilidade_venda_com_premio'] = consolidado['rentabilidade_venda_sem_premio'] + consolidado['Premios_recebidos']

    consolidado['variacao_ativo'] = consolidado.apply(
        lambda x: safe_div(x['preco_fechamento'] - x['preco_fechamento_inicio_operacoes'], x['preco_fechamento_inicio_operacoes']), axis=1)

    # ------------------------------
    # Garantir que não haja duplicados antes de salvar
    # ------------------------------
    consolidado = consolidado.drop_duplicates(subset=['conta','cliente','ativo_base'])

    

    # ------------------------------
    # Salvar no banco
    # ------------------------------
    consolidado.to_sql('historico_operacoes', engine, if_exists='append', index=False)
    print("Histórico de operações atualizado com sucesso")



def atualizar_asset_yahoo(engine=None):
    if engine is None:
        from backend.conexao import conectar
        engine = conectar()
    ...
    df = pd.read_sql("SELECT asset_original FROM ativos_yahoo", engine)

    df['asset_yahoo'] = df['asset_original'].apply(lambda x: x.strip().upper() + ".SA")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            query = text("""
                UPDATE ativos_yahoo
                SET asset_yahoo = :asset_yahoo
                WHERE asset_original = :asset_original
            """)
            conn.execute(query, {
                "asset_yahoo": row["asset_yahoo"],
                "asset_original": row["asset_original"]
            })

    print("Coluna asset_yahoo atualizada com sucesso!")

def importar_ativos_livres(caminho_planilha):
    # Conectar ao banco
    engine = conectar()

    # Ler a planilha
    df = pd.read_excel(caminho_planilha)

    # Padronizar coluna 'Ativo' para garantir correspondência no JOIN
    if 'Ativo' in df.columns:
        df['Ativo'] = df['Ativo'].astype(str).str.strip().str.upper()

    # Garantir que colunas numéricas estejam no formato correto
    colunas_numericas = ['Qtde_Total', 'Qtde_livre', 'Preco_Medio', 'Preco_Atual', 'Rentabilidade']
    for col in colunas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Converte e trata erros como NaN

    # Visualização dos dados importados (útil para diagnóstico)
    print("Pré-visualização dos dados importados:")
    print(df[['Ativo', 'Preco_Medio', 'Preco_Atual']].head())

    # Limpar a tabela e inserir os dados
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM ativos_livres"))
        df.to_sql('ativos_livres', con=conn, if_exists='append', index=False)

        # Atualizar Preco_Atual com base na tabela ativos_yahoo
        conn.execute(text("""
            UPDATE ativos_livres AS al
            JOIN ativos_yahoo AS ay ON UPPER(TRIM(al.Ativo)) = UPPER(TRIM(ay.asset_original))
            SET al.Preco_Atual = ay.Preco_Atual
        """))

        # Calcular Rentabilidade apenas quando Preco_Medio é válido
        conn.execute(text("""
            UPDATE ativos_livres
            SET Rentabilidade = ROUND(((Preco_Atual - Preco_Medio) / Preco_Medio) * 100, 2)
            WHERE Preco_Medio IS NOT NULL AND Preco_Medio > 0
        """))

        # Zerar Rentabilidade onde Preco_Medio é inválido
        conn.execute(text("""
            UPDATE ativos_livres
            SET Rentabilidade = NULL
            WHERE Preco_Medio IS NULL OR Preco_Medio = 0
        """))

        #Calcular o volume Livre
        conn.execute(text("""
            UPDATE ativos_livres
            SET Volume_Livre = ROUND(Qtde_livre * Preco_Atual, 2)
            WHERE Qtde_livre IS NOT NULL AND Preco_Atual IS NOT NULL
        """))

def atualizar_preco_atual_ativos_livres():
    engine = conectar()
    etapas = [
        "Atualizando preços dos ativos...",
        "Calculando rentabilidade válida...",
        "Zerando rentabilidade inválida...",
        "Calculando volume livre..."
    ]

    progress_bar = st.progress(0)
    status_text = st.empty()

    with engine.begin() as conn:
        # Etapa 1
        status_text.text(etapas[0])
        conn.execute(text("""
            UPDATE ativos_livres AS al
            JOIN ativos_yahoo AS ay
              ON UPPER(TRIM(al.Ativo)) = UPPER(TRIM(ay.asset_original))
            SET al.Preco_Atual = ay.preco_atual
        """))
        progress_bar.progress(0.25)

        # Etapa 2
        status_text.text(etapas[1])
        conn.execute(text("""
            UPDATE ativos_livres
            SET Rentabilidade = ROUND(((Preco_Atual - Preco_Medio) / Preco_Medio) * 100, 2)
            WHERE Preco_Medio IS NOT NULL AND Preco_Medio > 0
              AND Preco_Atual IS NOT NULL
        """))
        progress_bar.progress(0.5)

        # Etapa 3
        status_text.text(etapas[2])
        conn.execute(text("""
            UPDATE ativos_livres
            SET Rentabilidade = NULL
            WHERE Preco_Medio IS NULL OR Preco_Medio = 0
        """))
        progress_bar.progress(0.75)

        # Etapa 4
        status_text.text(etapas[3])
        conn.execute(text("""
            UPDATE ativos_livres
            SET Volume_Livre = ROUND(Qtde_livre * Preco_Atual, 2)
            WHERE Qtde_livre IS NOT NULL AND Preco_Atual IS NOT NULL
        """))
        progress_bar.progress(1.0)

    status_text.text("✅ Atualização concluída com sucesso.")
    st.success("Todos os dados foram atualizados com sucesso.")


def consolidar_notas_simples(data_inicio, data_fim, engine):
    import pandas as pd

    notas = pd.read_sql("""
        SELECT conta, cliente, ativo_base, tipo_papel, tipo_lado, quantidade, valor_operacao, data_registro
        FROM notas
        WHERE data_registro BETWEEN %s AND %s
    """, engine, params=(data_inicio, data_fim))

    # Separar ações e opções
    acoes = notas[notas['tipo_papel'] == 'ACAO']
    opcoes = notas[notas['tipo_papel'] == 'OPCAO']

    # Agrupamento de compras e vendas
    compras = acoes[acoes['tipo_lado'] == 'C'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Quantidade_comprada=('quantidade', 'sum'),
        Total_compras=('valor_operacao', 'sum')
    )

    vendas = acoes[acoes['tipo_lado'] == 'V'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Quantidade_vendida=('quantidade', 'sum'),
        Total_vendas=('valor_operacao', 'sum')
    )

    # Prêmios
    premios_recebidos = opcoes[opcoes['tipo_lado'] == 'V'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Premio_recebido=('valor_operacao', 'sum')
    )

    premios_pagos = opcoes[opcoes['tipo_lado'] == 'C'].groupby(['conta', 'cliente', 'ativo_base']).agg(
        Premio_pago=('valor_operacao', 'sum')
    )

    # Consolidação
    consolidado = compras.join(vendas, how='outer').join(premios_recebidos, how='outer').join(premios_pagos, how='outer')
    consolidado = consolidado.fillna(0)

    consolidado['quantidade_atual'] = consolidado['Quantidade_comprada'] - consolidado['Quantidade_vendida']
    consolidado['Premio_liquido'] = consolidado['Premio_recebido'] - consolidado['Premio_pago']

    consolidado = consolidado.reset_index()
    return consolidado





