from db_model import CotacaoAcao, HistoricoTicker, get_db, create_db_and_tables
from sqlalchemy import tuple_ # <-- Importante: Importar 'tuple_'

# --- Cria tabelas se ainda não existirem ---
create_db_and_tables()


def salvar_dados_no_db(dados):
    """Inserção/atualização na tabela cotacoes_acoes."""
    session = get_db()
    try:
        contagem_inseridos = 0
        contagem_atualizados = 0

        for item in dados:
            acao_nome = item.get("Acao")
            if not acao_nome:
                print(f"Ignorando item sem 'Acao': {item}")
                continue

            existente = session.query(CotacaoAcao).filter_by(Acao=acao_nome).first()
            if existente:
                for chave, valor in item.items():
                    setattr(existente, chave, valor)
                contagem_atualizados += 1
            else:
                nova_cotacao = CotacaoAcao(**item)
                session.add(nova_cotacao)
                contagem_inseridos += 1

        session.commit()
        print(f"SUCESSO (CotacaoAcao): {contagem_inseridos} inseridos, {contagem_atualizados} atualizados.")

    except Exception as e:
        session.rollback()
        print(f"ERRO no merge (CotacaoAcao): {e}")
    finally:
        session.close()

def salvar_historico_no_db(dados):
    """
    Otimizado: Inserção/atualização em lote (bulk) na tabela HistoricoTicker.
    """
    session = get_db()
    total_registros = len(dados)
    
    # Listas para operações em lote
    dados_a_inserir = []
    dados_a_atualizar = []
    
    print(f"\n-> Iniciando Preparação para Inserção/Atualização em Lote. Total de registros: {total_registros}")

    try:
        # 1. PRÉ-PROCESSAMENTO: Separar dados em "Inserir" e "Atualizar"
        
        # Cria um set de tuplas (Date, Ticker) para consulta eficiente
        chaves_para_verificar = [(item.get("Date"), item.get("Ticker")) 
                                 for item in dados if item.get("Date") and item.get("Ticker")]
        
        # Consulta de existência em lote (Bulk check)
        # CORREÇÃO AQUI: Usar tuple_ para a cláusula IN com múltiplas colunas
        registros_existentes = session.query(HistoricoTicker.Date, HistoricoTicker.Ticker) \
                                      .filter(
                                          tuple_(HistoricoTicker.Date, HistoricoTicker.Ticker).in_(chaves_para_verificar)
                                      ).all()
                                      
        # Converte a lista de tuplas existentes em um conjunto para pesquisa O(1)
        chaves_existentes = set(registros_existentes)
        
        # Preenche as listas de bulk
        for item in dados:
            date = item.get("Date")
            ticker = item.get("Ticker")
            
            if not date or not ticker:
                continue

            chave = (date, ticker)
            
            if chave in chaves_existentes:
                dados_a_atualizar.append(item)
            else:
                dados_a_inserir.append(item)

        # 2. OPERAÇÕES EM LOTE (BULK)
        
        # ... (Resto do código de bulk_insert e bulk_update) ...
        
        # Inserção em Lote
        if dados_a_inserir:
            session.bulk_insert_mappings(HistoricoTicker, dados_a_inserir)
            print(f"   [BULK] {len(dados_a_inserir)} registros preparados para INSERÇÃO.")

        # Atualização em Lote
        if dados_a_atualizar:
            session.bulk_update_mappings(HistoricoTicker, dados_a_atualizar)
            print(f"   [BULK] {len(dados_a_atualizar)} registros preparados para ATUALIZAÇÃO.")


        # 3. COMMIT ÚNICO
        session.commit()
        
        print(f"\nSUCESSO (HistoricoTicker): {len(dados_a_inserir)} inseridos, {len(dados_a_atualizar)} atualizados. (Total: {total_registros})")

    except Exception as e:
        session.rollback()
        print(f"\nERRO no bulk (HistoricoTicker). Rollback efetuado: {e}")
    finally:
        session.close()