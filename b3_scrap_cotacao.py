import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from limpar import _limpar_e_converter_dados
from processar_volume import _processar_volume

ARQUIVO = "altas_baixas_b3.csv"

def extrair_dados_acoes():
    
    """
    Função para fazer web scraping usando Selenium para carregar a página
    e BeautifulSoup para analisar os dados.
    """
    url = "https://www.infomoney.com.br/ferramentas/altas-e-baixas/"

    # Configurações do Selenium para rodar em modo "headless" (sem abrir janela do navegador)
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

    # Inicia o driver do Chrome
    # O Service() tentará baixar e gerenciar o chromedriver automaticamente
    try:
        service = Service()
        driver = webdriver.Chrome(service=service, options=chrome_options)

        # Navega até a página
        print("Acessando a página com o Selenium...")
        driver.get(url)

        # ESPERA IMPORTANTE: Dá tempo para o JavaScript carregar os dados.
        # 5 segundos costuma ser suficiente, mas pode ser ajustado.
        print("Aguardando o carregamento dinâmico dos dados...")
        time.sleep(5) 

        # Pega o HTML da página DEPOIS que o JavaScript rodou
        html_completo = driver.page_source

        print("Página carregada. Analisando o HTML com BeautifulSoup...")
        
    finally:
        # Garante que o navegador seja fechado mesmo se ocorrer um erro
        driver.quit()

    # Agora, o resto do código é o mesmo de antes, mas usando o HTML completo
    soup = BeautifulSoup(html_completo, 'html.parser')

    tabela = soup.find('table', id='altas_e_baixas')

    if tabela is None:
        print("ERRO CRÍTICO: Mesmo com o Selenium, a tabela não foi encontrada.")
        return []

    # Desta vez, o tbody DEVE existir
    tbody = tabela.find('tbody')
    if tbody is None:
        print("ERRO CRÍTICO: Mesmo com o Selenium, o 'tbody' não foi encontrado.")
        return []

    dados_acoes = []
    for linha in tbody.find_all('tr'):
        celulas = linha.find_all('td')
        if celulas and celulas[0].find('a'):
            nome_acao = celulas[0].find('a').get_text(strip=True)
            outras_informacoes = [celula.get_text(strip=True) for celula in celulas]

            # Mapeia os dados com base na ordem das colunas visíveis no site
            acao_info = {
                "Acao": outras_informacoes[0],
                "Hora": outras_informacoes[1],
                "Ultima": _limpar_e_converter_dados(outras_informacoes[2]), # FLOAT
                "VarDia": _limpar_e_converter_dados(outras_informacoes[3]), # FLOAT
                "VarSemana": _limpar_e_converter_dados(outras_informacoes[4]), # FLOAT
                "VarMes": _limpar_e_converter_dados(outras_informacoes[5]), # FLOAT
                "VarAno": _limpar_e_converter_dados(outras_informacoes[6]), # FLOAT
                "MinDia": _limpar_e_converter_dados(outras_informacoes[7]), # FLOAT
                "MaxDia": _limpar_e_converter_dados(outras_informacoes[8]), # FLOAT
                "Volume": _processar_volume(outras_informacoes[9]), # STRING
            }
            dados_acoes.append(acao_info)

    return dados_acoes

ARQUIVO_CSV = "altas_baixas_b3.csv"

def salvar_dados_em_csv(dados):
    """
    Converte a lista de dicionários em um DataFrame do Pandas e salva como CSV.
    """
    if not dados:
        print("Nenhum dado para salvar em CSV.")
        return

    try:
        # Cria um DataFrame a partir da lista de dicionários
        df = pd.DataFrame(dados)
        
        # Salva o DataFrame no arquivo CSV
        # index=False evita salvar a coluna de índice do Pandas
        # mode='a' para anexar se o arquivo existir, header=False se não for o primeiro save.
        
        # Para salvar do zero (o mais comum):
        df.to_csv(ARQUIVO_CSV, index=False, sep=';', decimal=',')
        
        # Se você quiser APPEND (anexar) ao mesmo arquivo para manter o histórico:
        # df.to_csv(ARQUIVO_CSV, index=False, header=False, mode='a', sep=';', decimal=',')


        print(f"SUCESSO: {len(dados)} cotações salvas em {ARQUIVO_CSV}")
        
    except Exception as e:
        print(f"ERRO ao salvar CSV: {e}")


def rodar_pipeline_csv():
    """ Orquestra a extração e o salvamento em CSV. """
    print("=" * 40)
    print("INICIANDO PIPELINE DE SCRAPING PARA CSV")
    print("=" * 40)

    # 1. Extração dos dados
    dados_extraidos = extrair_dados_acoes()

    # 2. Salvamento em CSV
    salvar_dados_em_csv(dados_extraidos)

    print("\nPIPELINE CONCLUÍDO.")
    print("=" * 40)


if __name__ == "__main__":
    rodar_pipeline_csv()