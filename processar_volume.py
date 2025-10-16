def _processar_volume(volume_texto):
    """Converte valores de volume abreviados (M, B) para float."""
    
    texto_limpo = volume_texto.upper().strip() # Garante que M/B seja maiúsculo

    if 'M' in texto_limpo:
        # Multiplica por 1 milhão
        valor = texto_limpo.replace('M', '').replace(',', '.').strip()
        try:
            return float(valor) * 1_000_000
        except ValueError:
            return None
            
    elif 'B' in texto_limpo:
        # Multiplica por 1 bilhão
        valor = texto_limpo.replace('B', '').replace(',', '.').strip()
        try:
            return float(valor) * 1_000_000_000
        except ValueError:
            return None
            
    else:
        # Tenta converter o valor diretamente se não tiver M ou B
        try:
            return float(texto_limpo.replace(',', '.'))
        except ValueError:
            return None