# scraper.py

def _limpar_e_converter_dados(dado_texto):
    """Converte strings com formato B3 para números float."""
    
    # Se o valor for um traço de nulo, retorna None (que é aceito pelo FLOAT)
    if dado_texto.strip() == '-':
        return None
        
    # Remove símbolos de variação (+, -), '%' e espaços
    # O replace('.', '') é CRÍTICO para números grandes (ex: 1.000,00)
    # Mas é perigoso em números pequenos (ex: 0.89)
    # Como o InfoMoney usa 0,89 e 9.2M (para Volume), vamos focar na vírgula e porcentagem.
    
    dado_limpo = dado_texto.replace('+', '').replace('-', '').replace('%', '').replace(',', '.').strip()
    
    # Se a string vaziar após a limpeza, também retorna None
    if not dado_limpo:
        return None

    try:
        return float(dado_limpo)
    except ValueError:
        # Se ainda falhar, imprima o valor problemático para depuração
        print(f"Alerta: Não foi possível converter o valor final '{dado_texto}' para float.")
        return None