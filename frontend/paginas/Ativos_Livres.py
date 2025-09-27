import streamlit as st
import pandas as pd
from importacao import (
    importar_ativos_livres,
    atualizar_preco_atual_ativos_livres,
    engine
)

    def render():
        # 🔒 Verifica se o usuário está logado e tem perfil permitido
        if "usuario" not in st.session_state or st.session_state.usuario is None:
            st.warning("🔒 Você precisa estar logado.")
            st.stop()
    
        usuario = st.session_state.usuario
        if usuario["perfil"] not in ["admin", "usuario"]:
            st.warning("🔒 Acesso restrito.")
            st.stop()

        
    
    st.set_page_config(page_title="Dashboard de Ativos", layout="wide")
    st.title("📊 Dashboard de Ativos Livres")

    # 🔄 Atualizar preços atuais dos ativos livres
    if st.button("🔄 Atualizar preços atuais"):
        try:
            atualizar_preco_atual_ativos_livres()
            st.success("✅ Preços atualizados com sucesso!")
        except Exception as e:
            st.error(f"❌ Erro ao atualizar preços: {e}")
        

    # Conectar e carregar dados
    df = pd.read_sql("SELECT * FROM ativos_livres", con=engine)

    if df.empty:
        st.warning("Nenhum dado encontrado.")
    else:
        # Agrupamento de filtros
        st.markdown("### 🔍 Filtros de Identificação")
        col1, col2, col3, col4 = st.columns(4)
        cliente_busca = col1.text_input("Buscar Cliente")
        ativo_sel = col2.selectbox("Ativo", ["Todos"] + sorted(df['Ativo'].dropna().unique()))
        assessor_sel = col3.text_input("Buscar por Assessor")
        mesa_sel = col4.selectbox("Mesa", ["Todos"] + sorted(df['Mesa'].dropna().unique()))

        st.markdown("---")

        st.markdown("### 📦 Filtros Numéricos")
        col5, col6 = st.columns(2)
        qtde_minima = col5.number_input("Qtde Livre mínima", min_value=0, value=0)
        volume_minimo = col6.number_input("Volume Livre mínima", min_value=0.0, value=0.0)

        # Aplicar filtros
        if cliente_busca:
            df = df[df['Cliente'].str.contains(cliente_busca, case=False, na=False)]
        if ativo_sel != "Todos":
            df = df[df['Ativo'] == ativo_sel]
        if assessor_sel != "Todos":
            df = df[df['Assessor'].str.contains(assessor_sel, case=False, na=False)]
        if mesa_sel != "Todos":
            df = df[df['Mesa'] == mesa_sel]
        df = df[df['Qtde_Livre'].fillna(0) > qtde_minima]
        df = df[df['Volume_Livre'].fillna(0) > volume_minimo]

        # Calcular soma do Volume Livre filtrado
        volume_total = df['Volume_Livre'].sum(skipna=True)

        # Exibir como métrica no topo
        st.metric(label="💰 Volume Livre Total (filtrado)", value=f"R$ {volume_total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        df = df.sort_values(by='Volume_Livre', ascending=False, na_position='last')


        # Formatação
        def format_brl(x):
            return f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        def format_pct(x):
            return f"{x:.2f} %".replace(".", ",")

        df_formatado = df.copy()
        df_formatado['Preco_Medio'] = df_formatado['Preco_Medio'].apply(format_brl)
        df_formatado['Preco_Atual'] = df_formatado['Preco_Atual'].apply(format_brl)
        df_formatado['Volume_Livre'] = df_formatado['Volume_Livre'].apply(format_brl)
        df_formatado['Rentabilidade'] = df_formatado['Rentabilidade'].apply(format_pct)

        # Seleção de colunas e renomeação
        colunas_exibir = ['Conta', 'Cliente', 'Ativo', 'Assessor', 'Qtde_Total', 'Qtde_Livre',
                        'Preco_Medio', 'Preco_Atual', 'Volume_Livre', 'Rentabilidade']

        nomes_personalizados = {
            'Conta': 'Código da Conta',
            'Cliente': 'Nome do Cliente',
            'Ativo': 'Código do Ativo',
            'Assessor': 'Assessor Responsável',
            'Qtde_Total': 'Quantidade Total',
            'Qtde_Livre': 'Quantidade Livre',
            'Preco_Medio': 'Preço Médio (R$)',
            'Preco_Atual': 'Preço Atual (R$)',
            'Volume_Livre': 'Volume Livre (R$)',
            'Rentabilidade': 'Rentabilidade (%)'
        }

        df_final = df_formatado[colunas_exibir].rename(columns=nomes_personalizados)

        st.markdown("---")
        st.subheader("📋 Tabela de Ativos Formatada")
        st.dataframe(df_final, use_container_width=True)

        st.caption(f"🔎 {len(df_final)} ativos encontrados com os filtros aplicados.")
