import api
import utils
from extractor import InfraspeakExtractor
import os
import re

utils.load_dotenv(utils.FILE_AUTH)

def processar_arquivo(arquivo_nome, resource_type, include_records, extractor):
    """Lê o arquivo, limpa a sujeira do SQL e aciona o extrator."""
    if not os.path.exists(arquivo_nome):
        # Se você não exportou esse arquivo, ele pula silenciosamente
        return 

    ids_limpos = []
    
    with open(arquivo_nome, 'r', encoding='utf-8') as f:
        for linha in f:
            # Extrai apenas os números
            id_limpo = re.sub(r'[^0-9]', '', linha)
            if id_limpo:
                ids_limpos.append(id_limpo)

    # Remove duplicatas
    ids_limpos = list(set(ids_limpos))
    total = len(ids_limpos)
    
    if total > 0:
        print(f"\n>>> Lendo '{arquivo_nome}': Iniciando repescagem de {total} {resource_type}s...")
        extractor.sync_details(ids_limpos, resource_type, include_records=include_records)
        print(f"✅ Repescagem de {resource_type} concluída!")

def rodar_repescagem_completa():
    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    api_client = api.ApiInfraspeak(user, token)
    extract = InfraspeakExtractor(api_client)

    print("--- Iniciando Operação de Repescagem Múltipla ---")
    
    # 1. Busca arquivo de Chamados (Failures)
    # include_records = True (para baixar os eventos/apontamentos)
    processar_arquivo('repescagem_failures.csv', 'failure', True, extract)
    
    # 2. Busca arquivo de Planos Mestres (Works)
    # include_records = False (planos mestres não têm eventos de horas)
    processar_arquivo('repescagem_works.csv', 'work', False, extract)
    
    # 3. Busca arquivo de Ocorrências Preventivas (Scheduled Works)
    # include_records = True (para baixar os eventos/apontamentos)
    processar_arquivo('repescagem_scheduled.csv', 'scheduled_work', True, extract)
    
    print("\nProcesso finalizado. Todos os arquivos disponíveis foram processados.")

if __name__ == "__main__":
    rodar_repescagem_completa()