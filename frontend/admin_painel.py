import streamlit as st
from importacao import (
    importar_notas_atualizado,
    importar_historico_precos,
    importar_proventos,
    importar_ativos,
    calcular_resultado_opcoes,
    importar_vencimentos_opcoes,
    atualizar_historico_operacoes,
    importar_ativos_yahoo,
    atualizar_asset_yahoo,
    importar_ativos_livres
)

def render():
    st.info("Acesso administrativo liberado.")
    st.subheader("📥 Importações e Atualizações")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Importar Notas de Corretagem"):
            importar_notas_atualizado()
            st.success("Notas de corretagem importadas com sucesso.")

        if st.button("Importar Histórico de Preços"):
            importar_historico_precos()
            st.success("Histórico de preços importado com sucesso.")

        if st.button("Importar Proventos"):
            importar_proventos()
            st.success("Proventos importados com sucesso.")

        if st.button("Importar Ativos"):
            importar_ativos()
            st.success("Ativos importados com sucesso.")

    with col2:
        if st.button("Calcular Resultado das Opções"):
            calcular_resultado_opcoes()
            st.success("Resultado das opções calculado com sucesso.")

        if st.button("Importar Vencimento das Opções"):
            importar_vencimentos_opcoes()
            st.success("Vencimentos importados com sucesso.")

        if st.button("Atualizar Ativos com código .SA"):
            atualizar_asset_yahoo()
            st.success("Ativos atualizados com sucesso.")

    st.subheader("📈 Atualizar Histórico de Operações")
    if st.button("Atualizar histórico"):
        try:
            atualizar_historico_operacoes()
            st.success("✅ Histórico atualizado com sucesso!")
        except Exception as e:
            st.error(f"❌ Erro ao atualizar: {e}")

    st.subheader("📤 Importar Ativos do Yahoo Finance")
    arquivo = st.file_uploader("Selecione o arquivo de ativos", type=['csv', 'xlsx'])
    if arquivo:
        tipo = 'csv' if arquivo.name.endswith('.csv') else 'excel'
        caminho_temp = f"temp_{arquivo.name}"
        with open(caminho_temp, "wb") as f:
            f.write(arquivo.getbuffer())

        if st.button("Importar ativos"):
            mensagem = importar_ativos_yahoo(caminho_temp, tipo)
            if "sucesso" in mensagem.lower():
                st.success(mensagem)
            else:
                st.error(mensagem)

    st.subheader("📤 Importar Ativos Livres")
    arquivo_livres = st.file_uploader("Selecione a planilha de ativos livres", type=["xlsx"])
    if st.button("Importar dados"):
        if arquivo_livres is not None:
            importar_ativos_livres(arquivo_livres)
            st.success("Dados importados e atualizados com sucesso!")
        else:
            st.warning("Por favor, selecione um arquivo antes de importar.")