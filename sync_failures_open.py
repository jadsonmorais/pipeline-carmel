import api_v2
import utils_v2
import extractor_v2
import os

def sync_open_failures():
    print("--- Sincronização de Chamados Abertos Iniciada ---")
    
    # Carregamento de credenciais
    utils_v2.load_dotenv(utils_v2.FILE_AUTH)
    api = api_v2.ApiInfraspeak(os.getenv('API_DATA_USER'), os.getenv('API_DATA_TOKEN'))
    
    # Busca apenas os chamados abertos via Bulk (Rápido)
    endpoint, params = api_v2.RouteManager.get_open_failures()
    
    try:
        response = api.request(endpoint, params)
        data = response.get('data', [])
        
        if data:
            print(f"Atualizando {len(data)} chamados abertos no banco...")
            # Upsert direto na tabela raw (Objetivo 1)
            utils_v2.upsert_raw_data('infraspeak_raw_failure_details', 'failure_id', data, 'failure')
            print("Sucesso!")
        else:
            print("Nenhum chamado aberto encontrado.")
            
    except Exception as e:
        print(f"Erro na sincronização de abertos: {e}")

if __name__ == "__main__":
    sync_open_failures()