import api
import utils
import json
import time


class InfraspeakExtractor:
    def __init__(self, api_client):
        self.api = api_client

    def sync_details(self, ids, resource_type, include_records=True):
        """
        Método unificado para buscar detalhes de Falhas ou Trabalhos.
        ids: Lista de IDs vindos do Bulk.
        resource_type: 'failure' ou 'work'.
        include_records: Define se deve trazer o events.registry (pesado).
        """
        if not ids:
            print(f"Nenhum ID de {resource_type} para processar.")
            return

        print(f"Iniciando extração detalhada de {len(ids)} {resource_type}s...")

        # Configurações dinâmicas conforme o tipo de recurso
        if resource_type == 'failure':
            endpoint_prefix = "failures"
            table_name = "infraspeak_raw_failure_details"
            id_column = "failure_id"
            # Expansões para Falhas [1, 2]
            expansion = "operator,location,client,problem"
        else:
            endpoint_prefix = "works/scheduled"
            table_name = "infraspeak_raw_work_details"
            id_column = "work_id"
            # Expansões para Works/Scheduled [3, 4]
            expansion = "work.client,work.locations,work.operators,work.work_type,audit_stats.category"

        # Adiciona registros apenas se solicitado (Lógica do Ano Atual/Delta)
        if include_records:
            expansion += ",events.registry"

        for r_id in ids:
            try:
                params = {"expanded": expansion}
                # Chama o método request da classe ApiInfraspeak [5, 6]
                response = self.api.request(f"{endpoint_prefix}/{r_id}", params)
                
                # Prepara o JSON para o Upsert no Postgres [7]
                # O banco espera uma lista de dicionários com 'id' e o objeto completo
                response['id'] = response['data']['id']
                
                # Persistência Bruta (Objetivo 1) [8]
                utils.upsert_raw_data(table_name, id_column, [response], resource_type)
                
                # Pequena pausa para evitar sobrecarga de conexão (SSL Error) e respeitar o Throttling [9, 10]
                time.sleep(0.4) 
                
            except Exception as e:
                print(f" [ERRO] Falha ao detalhar {resource_type} {r_id}: {e}")
                continue
        
        print(f"Sincronização de detalhes de {resource_type} concluída.")


    def sync_all_failure_details(self, failure_ids):
        """Busca registros detalhados (events.registry) para cada ID [7, 8]"""
        print(f"Iniciando extração detalhada para {len(failure_ids)} IDs...")
        
        for f_id in failure_ids:
            endpoint = f"failures/{f_id}"
            # Expandido para trazer os registros de mudança de status [9]
            params = {"expanded": "events.registry"}
            
            try:
                response = self.api.request(endpoint, params)
                
                # Injeta o ID na raiz para evitar o KeyError identificado nos testes
                response['id'] = response['data']['id']
                
                # Salva o JSON completo (incluindo o nó 'included' com os registros) [3, 10]
                utils.upsert_raw_data(
                    table_name='infraspeak_raw_failure_details', 
                    id_column='failure_id', 
                    data_list=[response], 
                    type='failure'
                )
            except Exception as e:
                print(f"Erro ao processar detalhes do ID {f_id}: {e}")


    def sync_works_bulk(self, date_start, date_end=None):
        """Busca a lista de ocorrências (Scheduled Works) do período"""
        # A rota correta para ocorrências é 'works/scheduled' [2-4]
        endpoint = "works/scheduled"
        params = {
            "date_min_start_date": f"{date_start}T00:00:00",
            "limit": 200,
            "expanded": "work" # Traz informações básicas do trabalho pai [2]
        }
        if date_end:
            params["date_max_start_date"] = f"{date_end}T23:59:59"

        print(f"Buscando lista de ocorrências em {date_start}...")
        # Usa o seu método que trata Throttling e retorna o JSON [5]
        response = self.api.request(endpoint, params)
        data = response.get('data', [])
        
        if data:
            # Salva o bulk básico na tabela de listagem [6]
            utils.upsert_raw_data('infraspeak_raw_works', 'work_id', data, 'work')
            return data
        return []

    def sync_all_work_details(self, work_ids):
        """Implementa o loop de IDs usando a URL detalhada que você definiu"""
        for sw_id in work_ids:
            # Usamos a exata configuração de expansão do seu snippet
            endpoint = f"works/scheduled/{sw_id}"
            params = {
                "expanded": "audit_stats.category,scheduled_work_slas.calendar,work.client,"
                            "work.locations,work.operators,work.supplier.handshake,"
                            "work.technical_skills,work.work_type,events.registry",
                "include_survey_avg_rating": "true"
            }
            
            try:
                # A chamada api.request já trata o limite de 60 req/min (Throttling) [9, 10]
                response = self.api.request(endpoint, params)
                
                # Persistência Bruta: Agora o JSON terá o nó 'included' populado
                response['id'] = response['data']['id']
                utils.upsert_raw_data('infraspeak_raw_work_details', 'work_id', [response], 'work')
                
            except Exception as e:
                print(f"Erro ao detalhar ocorrência {sw_id}: {e}")