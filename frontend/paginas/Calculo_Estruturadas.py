def calcular_resultados(engine, df):
    # Corrige para usar preco_ultimo do historico_precos
    df_precos = pd.read_sql(
        text("SELECT codigo_bdi, preco_ultimo, data_pregao FROM historico_precos"),
        engine
    )
    df = df.copy()
    # Corrige colunas do df também para preco_ultimo
    for col in ['preco_ultimo', 'resultado']:
        if col not in df.columns:
            df[col] = pd.NA

    atualizacoes = []

    for idx, row in df.iterrows():
        # Corrige testes para preco_ultimo
        if pd.notna(row.get('preco_ultimo')) and pd.notna(row.get('resultado')):
            continue

        ativo = row['Ativo']
        vencimento = row['Data_Vencimento']
        st.write(f"Processando Ativo: {ativo}, Vencimento: {vencimento}")

        precos_ativos = df_precos[
            (df_precos['codigo_bdi'] == ativo) &
            (pd.to_datetime(df_precos['data_pregao']) == pd.to_datetime(vencimento))
        ]

        st.write(f"Preços encontrados: {len(precos_ativos)}")

        if not precos_ativos.empty:
            # Corrige referência aqui também
            preco_ult = precos_ativos['preco_ultimo'].iloc[0]
            df.at[idx, 'preco_ultimo'] = preco_ult

            strike = row.get('Valor_Strike_1', 0)
            qtd = row.get('Quantidade_Ativa_1', 0)
            custo_unit = row.get('Custo_Unitario_Cliente', 0)
            preco_atual = preco_ult

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
                'preco_ultimo': preco_ult,
                'resultado': resultado,
            })

    st.write(f"Total atualizações: {len(atualizacoes)}")

    with engine.begin() as conn:
        for atualizacao in atualizacoes:
            if atualizacao['id'] is not None:
                conn.execute(text("""
                    UPDATE operacoes_estruturadas
                    SET preco_ultimo = :preco, resultado = :resultado
                    WHERE id = :id
                """), {
                      'preco': atualizacao['preco_ultimo'],
                      'resultado': atualizacao['resultado'],
                      'id': atualizacao['id']
                })
    st.success(f"Foram atualizados {len(atualizacoes)} registros.")
    return df
