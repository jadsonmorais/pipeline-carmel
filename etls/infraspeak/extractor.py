from . import api
from shared import db as utils
import json
import time
from pathlib import Path
from datetime import datetime

LOG_PATH = Path(__file__).parent / 'ids_perdidos_detalhe.log'


class InfraspeakExtractor:
    def __init__(self, api_client):
        self.api = api_client

    def sync_details(self, ids, resource_type, include_records=True):
        """Método unificado com Retry (Resiliência) para buscar detalhes."""
        if not ids:
            return

        print(f"Iniciando extração detalhada de {len(ids)} {resource_type}s...")

        if resource_type == 'failure':
            endpoint_prefix = "failures"
            table_name = "infraspeak_raw_failure_details"
            id_column = "failure_id"
            expansion = "operator,location,client,problem"
            if include_records: expansion += ",events.registry"

        elif resource_type == 'work':
            endpoint_prefix = "works"
            table_name = "infraspeak_raw_work_details"
            id_column = "work_id"
            expansion = "workPeriodicity,workSlaRules,workType,client,locations,operators"

        elif resource_type == 'scheduled_work':
            endpoint_prefix = "works/scheduled"
            table_name = "infraspeak_raw_scheduled_work_details"
            id_column = "scheduled_work_id"
            expansion = "work.client,work.locations,work.operators,work.work_type,audit_stats.category"
            if include_records: expansion += ",events.registry"

        else:
            print("Tipo não suportado.")
            return

        for r_id in ids:
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    params = {"expanded": expansion}
                    response = self.api.request(f"{endpoint_prefix}/{r_id}", params)

                    # Validação de integridade do payload
                    if 'data' not in response:
                        raise ValueError(f"Payload inválido ou vazio retornado pela API.")

                    response['id'] = response['data']['id']

                    # Gravando no banco
                    utils.upsert_raw_data(table_name, id_column, [response], resource_type)
                    time.sleep(0.4)
                    break

                except Exception as e:
                    if attempt < max_retries - 1:
                        sleep_time = 2 ** (attempt + 1)
                        print(f"   [AVISO] Instabilidade no {resource_type} ID {r_id} (Tentativa {attempt + 1}/{max_retries}). Retentando em {sleep_time}s... Erro: {e}")
                        time.sleep(sleep_time)
                    else:
                        print(f"   [ERRO FATAL] Falha definitiva no {resource_type} ID {r_id} após {max_retries} tentativas.")
                        with open(LOG_PATH, "a", encoding="utf-8") as f:
                            agora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            f.write(f"{agora} | {resource_type} | ID: {r_id} | Erro: {e}\n")
                        # Marca como EXCLUIDO no banco se for 404 em failure
                        if resource_type == 'failure' and '404' in str(e):
                            utils.mark_failure_as_deleted(r_id)
                            print(f"   [INFO] failure ID {r_id} marcado como EXCLUIDO no banco.")
