import api_v2
import utils_v2
import extractor_v2
import os
import sys
import time
from datetime import datetime, timedelta
from utils_v2 import load_dotenv

# Carrega as credenciais [4, 5]
load_dotenv(utils_v2.FILE_AUTH)

def get_days_list(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    delta = end_date - start_date
    return [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]

def run_historical_sync(date_start, date_end, include_records=False):
    print(f"--- Iniciando Carga Histórica Otimizada: {date_start} até {date_end} ---")
    
    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    api_client = api_v2.ApiInfraspeak(user, token)
    extractor = extractor_v2.InfraspeakExtractor(api_client)
    
    dias = get_days_list(date_start, date_end)
    hoje = datetime.today().date()
    
    for dia in dias:
        print(f"\n>>> Processando dia: {dia}")
        try:
            data_dia = datetime.strptime(dia, '%Y-%m-%d').date()

            processa_failures = True
        
            if data_dia > hoje:
                processa_failures = False

            # 1. FAILURES - Processamento em Bloco (Fast)
            if processa_failures:
                print(f"[{dia}] Baixando Bloco de Failures...")
                # Usamos a rota bulk que já traz expansões básicas [6]
                f_endpoint, f_params = api_v2.RouteManager.get_failures_bulk(dia, dia)
                failures_response = api_client.request(f_endpoint, f_params)
                failures_data = failures_response.get('data', [])
                
                if failures_data:
                    if include_records:
                        # Se explicitamente pedido, faz o processo lento (ID por ID)
                        f_ids = [f['id'] for f in failures_data]
                        extractor.sync_details(f_ids, 'failure', include_records=True)
                    else:
                        # UPSERT DIRETO NO LOTE (Extremamente rápido) [7]
                        print(f"[{dia}] Gravando {len(failures_data)} falhas no banco...")
                        utils_v2.upsert_raw_data('infraspeak_raw_failure_details', 'failure_id', failures_data, 'failure')

            # 2. SCHEDULED WORKS - Processamento em Bloco (Fast)
            print(f"[{dia}] Baixando Bloco de Scheduled Works...")
            w_endpoint = "works/scheduled"
            w_params = {
                "date_min_start_date": f"{dia}T00:00:00",
                "date_max_start_date": f"{dia}T23:59:59",
                "expanded": "work.client,work.locations,work.operators,work.work_type", # Expansões básicas inclusas
                "limit": 200
            }
            works_response = api_client.request(w_endpoint, w_params)
            works_data = works_response.get('data', [])
            
            if works_data:
                if include_records:
                    w_ids = [w['id'] for w in works_data]
                    extractor.sync_details(w_ids, 'work', include_records=True)
                else:
                    print(f"[{dia}] Gravando {len(works_data)} ocorrências no banco...")
                    utils_v2.upsert_raw_data('infraspeak_raw_work_details', 'work_id', works_data, 'work')
            
            print(f"[{dia}] Dia finalizado.")

        except Exception as e:
            print(f"[{dia}] Erro: {e}")
            continue

if __name__ == "__main__":
    if len(sys.argv) >= 3:
        # Por padrão, o histórico NÃO puxa registros (records) para ser rápido
        inc_rec = False 
        if len(sys.argv) == 4 and sys.argv[3].lower() == 'true':
            inc_rec = True
            
        run_historical_sync(sys.argv[1], sys.argv[2], include_records=inc_rec)
    else:
        print("Uso: python history_sync_v2.py 2024-01-01 2024-01-31 [true/false]")