from . import api, extractor
from shared import db as utils
import os
import sys
import time
from datetime import datetime, timedelta


def get_days_list(start_date_str, end_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    delta = end_date - start_date
    return [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range(delta.days + 1)]


def run_historical_sync(date_start, date_end, include_records=False):
    print(f"--- Iniciando Carga Histórica Otimizada: {date_start} até {date_end} ---")

    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    api_client = api.ApiInfraspeak(user, token)
    extract = extractor.InfraspeakExtractor(api_client)

    dias = get_days_list(date_start, date_end)
    hoje = datetime.today().date()

    for dia in dias:
        print(f"\n>>> Processando dia: {dia}")
        try:
            data_dia = datetime.strptime(dia, '%Y-%m-%d').date()
            processa_failures = data_dia <= hoje

            # 1. FAILURES
            if processa_failures:
                print(f"[{dia}] Baixando Bloco de Failures...")
                f_endpoint, f_params = api.RouteManager.get_failures_bulk(dia, dia)
                failures_data = api_client.request_all_pages(f_endpoint, f_params)

                if failures_data:
                    print(f"[{dia}] Gravando {len(failures_data)} falhas brutas no banco...")
                    utils.upsert_raw_data('infraspeak_raw_failures', 'failure_id', failures_data, 'failure')

                    if include_records:
                        f_ids = [f['id'] for f in failures_data]
                        extract.sync_details(f_ids, 'failure', include_records=True)

            # 2. WORKS (MESTRE)
            print(f"[{dia}] Baixando Bloco de Works (Mestre)...")
            w_params = {
                "date_min_updated_at": f"{dia}T00:00:00",
                "date_max_updated_at": f"{dia}T23:59:59",
                "expanded": "workPeriodicity,workSlaRules,workType,client,locations",
                "limit": 200
            }
            works_data = api_client.request_all_pages("works", w_params)

            if works_data:
                utils.upsert_raw_data('infraspeak_raw_works', 'work_id', works_data, 'work')

                if include_records:
                    w_ids = [w['id'] for w in works_data]
                    extract.sync_details(w_ids, 'work', include_records=False)

            # 3. SCHEDULED WORKS (OCORRÊNCIAS)
            print(f"[{dia}] Baixando Bloco de Scheduled Works...")
            sw_params = {
                "date_min_start_date": f"{dia}T00:00:00",
                "date_max_start_date": f"{dia}T23:59:59",
                "expanded": "work.client,work.locations,work.operators,work.work_type",
                "limit": 200
            }
            sw_data = api_client.request_all_pages("works/scheduled", sw_params)

            if sw_data:
                utils.upsert_raw_data('infraspeak_raw_scheduled_works', 'scheduled_work_id', sw_data, 'scheduled_work')

                if include_records:
                    sw_ids = [sw['id'] for sw in sw_data]
                    extract.sync_details(sw_ids, 'scheduled_work', include_records=True)

        except Exception as e:
            print(f"[{dia}] Erro: {e}")
            continue


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        inc_rec = len(sys.argv) == 4 and sys.argv[3].lower() == 'true'
        run_historical_sync(sys.argv[1], sys.argv[2], include_records=inc_rec)
    else:
        print("Uso: python -m etls.infraspeak.history_sync 2024-01-01 2024-01-31 [true/false]")
