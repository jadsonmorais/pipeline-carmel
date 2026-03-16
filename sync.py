import api
import utils
import extractor
import os
from datetime import datetime, timedelta

# Carrega as credenciais
utils.load_dotenv(utils.FILE_AUTH)

def run_incremental_sync(days_back=3, include_records=True):
    date_start = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    print(f"--- Iniciando Carga Incremental (Delta) a partir de {date_start} ---")
    
    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    api_client = api.ApiInfraspeak(user, token)
    extract = extractor.InfraspeakExtractor(api_client)

    # 1. FAILURES
    print("\n>>> Sincronizando Failures recentes...")
    f_params = {
        "date_min_last_status_change_date": f"{date_start}T00:00:00",
        "expanded": "events,operator,location",
        "limit": 200
    }
    f_data = api_client.request_all_pages("failures", f_params) 
    if f_data:
        utils.upsert_raw_data('infraspeak_raw_failures', 'failure_id', f_data, 'failure')
        if include_records:
            f_ids = [f['id'] for f in f_data]
            extract.sync_details(f_ids, 'failure', include_records=True)

    # 2. WORKS (PLANOS MESTRES) - Parâmetro validado: date_min_updated_at
    print("\n>>> Sincronizando Works (Planos Mestres) recentes...")
    w_params = {
        "date_min_updated_at": f"{date_start}T00:00:00",
        "expanded": "workPeriodicity,workSlaRules,workType,client,locations",
        "limit": 200
    }
    w_data = api_client.request_all_pages("works", w_params)
    if w_data:
        utils.upsert_raw_data('infraspeak_raw_works', 'work_id', w_data, 'work')
        # Planos não precisam de extração de records
        w_ids = [w['id'] for w in w_data]
        extract.sync_details(w_ids, 'work', include_records=False)

    # 3. SCHEDULED WORKS (OCORRÊNCIAS) - Parâmetro validado: date_min_updated_at
    print("\n>>> Sincronizando Scheduled Works (Ocorrências) recentes...")
    sw_params = {
        "date_min_updated_at": f"{date_start}T00:00:00",
        "expanded": "work.client,work.locations,work.operators,work.work_type",
        "limit": 200
    }
    sw_data = api_client.request_all_pages("works/scheduled", sw_params)
    if sw_data:
        utils.upsert_raw_data('infraspeak_raw_scheduled_works', 'scheduled_work_id', sw_data, 'scheduled_work')
        if include_records:
            sw_ids = [sw['id'] for sw in sw_data]
            extract.sync_details(sw_ids, 'scheduled_work', include_records=True)
    
    # 4. OPERATORS (DIMENSÃO)
    print("\n>>> Sincronizando Operators (Operadores)...")
    op_params = {
        "limit": 200,
        # Você pode adicionar expansões caso a API permita relacionamentos, ex: "expanded": "teams,skills"
    }
    op_data = api_client.request_all_pages("operators", op_params)
    
    if op_data:
        utils.upsert_raw_data('infraspeak_raw_operators', 'operator_id', op_data, 'operator')
        print(f"Foram extraídos/atualizados {len(op_data)} operadores.")

    # 5. VALIDAÇÃO DE FAILURES PAUSADAS
    print("\n>>> Validando Failures com state PAUSED...")
    paused_ids = utils.get_failure_ids_by_state('PAUSED')
    if paused_ids:
        print(f"   {len(paused_ids)} failures pausadas encontradas. Verificando na API...")
        extract.sync_details(paused_ids, 'failure', include_records=True)
    else:
        print("   Nenhuma failure pausada encontrada.")

    print("\n--- Sincronização Delta Finalizada ---")

if __name__ == "__main__":
    # Roda os últimos 3 dias por padrão para sobrepor edições manuais feitas no fim de semana
    run_incremental_sync(days_back=3, include_records=True)