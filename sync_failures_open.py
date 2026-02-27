from api import RouteManager, ApiInfraspeak
import utils
import extractor
import os

def sync_open_failures():
    print("--- Sincronização de Chamados Abertos Iniciada ---")
    
    # Carregamento de credenciais
    utils.load_dotenv(utils.FILE_AUTH)
    api = ApiInfraspeak(os.getenv('API_DATA_USER'), os.getenv('API_DATA_TOKEN'))
    
    # Busca apenas os chamados abertos via Bulk (Rápido)
    endpoint, params = RouteManager.get_open_failures()
    
    try:
        response = api.request(endpoint, params)
        data = response.get('data', [])
        
        if data:
            print(f"Atualizando {len(data)} chamados abertos no banco...")
            # Upsert direto na tabela raw (Objetivo 1)
            utils.upsert_raw_data('infraspeak_raw_failures', 'failure_id', data, 'failure')
            print("Sucesso!")
        else:
            print("Nenhum chamado aberto encontrado.")
            
    except Exception as e:
        print(f"Erro na sincronização de abertos: {e}")

if __name__ == "__main__":
    sync_open_failures()