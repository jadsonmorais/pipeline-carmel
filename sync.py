from api_v2 import RouteManager, ApiInfraspeak
import utils_v2
import extractor_v2 as extractor # Importa o módulo
import os
from datetime import datetime, timedelta
from utils_v2 import load_dotenv

# Carrega as credenciais do ambiente (.env)
load_dotenv(utils_v2.FILE_AUTH)

def run_daily_sync():
    # 1. Inicialização das Credenciais e Objetos (O QUE ESTAVA FALTANDO)
    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    
    # Inicializa a conexão com a API [1]
    api_client = ApiInfraspeak(user, token)
    
    # Inicializa o extrator (passando o cliente da API)
    # Certifique-se de que a classe no extractor_v2 se chama InfraspeakExtractor
    ext = extractor.InfraspeakExtractor(api_client)
    
    # 2. Configuração de Datas
    hoje = datetime.now()
    tres_dias_atras = (hoje - timedelta(days=3)).strftime('%Y-%m-%d')
    
    # 3. FAILURES (3 dias atrás com registros)
    print("Sincronizando Failures (Delta 3 dias + Registros)...")
    endpoint, params = RouteManager.get_failures_delta(tres_dias_atras)
    # Usamos o objeto api_client inicializado acima
    failures = api_client.request(endpoint, params).get('data', [])
    # Usamos o objeto 'ext' inicializado acima
    ext.sync_details([f['id'] for f in failures], 'failure', include_records=True)

    # 4. SCHEDULED WORKS - DELTA (3 dias atrás com registros)
    print("Sincronizando Scheduled Works (Delta 3 dias + Registros)...")
    endpoint, params = RouteManager.get_scheduled_works_delta(tres_dias_atras)
    works_delta = api_client.request(endpoint, params).get('data', [])
    ext.sync_details([w['id'] for w in works_delta], 'work', include_records=True)

    # 5. SCHEDULED WORKS - FUTURE (Bulk sem registros - Performance)
    print("Sincronizando Scheduled Works (Futuro - Sem Registros)...")
    endpoint, params = RouteManager.get_scheduled_works_future()
    works_future = api_client.request(endpoint, params).get('data', [])
    ext.sync_details([w['id'] for w in works_future], 'work', include_records=False)

if __name__ == "__main__":
    run_daily_sync()