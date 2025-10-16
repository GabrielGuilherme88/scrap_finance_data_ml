from db_model import CotacaoAcao, get_db

def salvar_dados_no_db(dados):
    session = get_db()
    try:
        contagem_inseridos = 0
        contagem_atualizados = 0

        for item in dados:
            acao_nome = item.get("Acao")

            if not acao_nome:
                print(f"Ignorando item sem 'Acao': {item}")
                continue

            # Tenta encontrar ação existente
            existente = session.query(CotacaoAcao).filter_by(Acao=acao_nome).first()

            if existente:
                # Atualiza os campos do registro existente
                for chave, valor in item.items():
                    setattr(existente, chave, valor)
                contagem_atualizados += 1
            else:
                nova_cotacao = CotacaoAcao(**item)
                session.add(nova_cotacao)
                contagem_inseridos += 1

        session.commit()
        print(f"SUCESSO: {contagem_inseridos} inseridos, {contagem_atualizados} atualizados.")

    except Exception as e:
        session.rollback()
        print(f"ERRO no merge: {e}")
    finally:
        session.close()