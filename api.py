import requests
import time
import utils as utils

class ApiInfraspeak:
    def __init__(self, user, token):
        self.user = user
        self.token = token
        self.base_url = "https://api.infraspeak.com/v3"
        self.headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'User-Agent': f'Infraspeak_V2_ETL ({self.user})'
        }

    def request(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        # Gerenciamento de Throttling (Limite de 60 req/min) [1, 4]
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Limite atingido. Aguardando {retry_after} segundos...")
            time.sleep(retry_after)
            return self.request(endpoint, params)
            
        response.raise_for_status()
        return response.json()
    
    
    def request_all_pages(self, endpoint, params=None):
        """Gerencia a paginação automaticamente [2-5]"""
        if params is None: params = {}
        params['page'] = 1
        all_data = []

        while True:
            response = self.request(endpoint, params) # Usa o seu método com throttling [6]
            data = response.get('data', [])
            all_data.extend(data)

            meta = response.get('meta', {})
            pagination = meta.get('pagination', {})
            total_pages = pagination.get('total_pages', 1)

            if params['page'] >= total_pages:
                break
            
            params['page'] += 1
            print(f"Extraindo página {params['page']} de {total_pages}...")
        
        return all_data
    
class RouteManager:
    """Gerencia a construção de queries JQL otimizadas"""
    
    @staticmethod
    def get_failures_delta(date_start):
        """Busca falhas alteradas nos últimos 3 dias"""
        return "failures", {
            "date_min_last_status_change_date": f"{date_start}T00:00:00",
            "limit": 200
        }

    @staticmethod
    def get_scheduled_works_delta(date_start):
        """Busca ocorrências (Scheduled) alteradas nos últimos 3 dias"""
        return "works/scheduled", {
            "date_min_last_status_change_date": f"{date_start}T00:00:00",
            "limit": 200
        }

    @staticmethod
    def get_scheduled_works_future():
        """Busca o planejamento futuro (hoje em diante)"""
        today = utils.datetime.now().strftime('%Y-%m-%d')
        return "works/scheduled", {
            "date_min_start_date": f"{today}T00:00:00",
            "limit": 200
        }
    
# class RouteManager:
    @staticmethod
    def get_works_bulk():
        """Captura a estrutura dos modelos de trabalho (Works)"""
        return "works", {
            "expanded": "workPeriodicity,workSlaRules,workType,client,locations",
            "limit": 200
        }

    
    @staticmethod
    def get_failures_bulk(date_start, date_end=None):
        # Busca falhas com expansão de eventos para evitar chamadas extras [5, 6]
        params = {
            "date_min_last_status_change_date": f"{date_start}T00:00:00",
            "expanded": "events,operator,location", 
            "limit": 200
        }
        if date_end:
            params["date_max_last_status_change_date"] = f"{date_end}T23:59:59"
        return "failures", params
    
    @staticmethod
    def get_open_failures():
        # Retorna o endpoint específico para abertos com expansões básicas, excluindo 'events'
        params = {
            "expanded": "operator,location,client,problem", # Sem events.registry para ser rápido
            "limit": 200
        }
        return "failures/open", params
#     @staticmethod
#     def get_scheduled_works_delta(date_start):
#         """Captura mudanças recentes (passado) em ocorrências agendadas [6, 7]"""
#         return "works/scheduled", {
#             "date_min_last_status_change_date": f"{date_start}T00:00:00",
#             "expanded": "work.client,work.locations,work.work_type", # Sem registros por padrão
#             "limit": 200
#         }

#     @staticmethod
#     def get_scheduled_works_future():
#         """Captura o planejamento futuro [8]"""
#         today = utils.datetime.now().strftime('%Y-%m-%d')
#         return "works/scheduled", {
#             "date_min_start_date": f"{today}T00:00:00",
#             "expanded": "work.client,work.locations,work.work_type",
#             "limit": 200,
#             "sort": "start_date"
#         }

#     @staticmethod
#     def get_works_future(days_ahead=30):
#         # Resolve o Problema 2: Busca trabalhos agendados para o futuro [2, 7]
#         today = time.strftime("%Y-%m-%d")
#         params = {
#             "date_min_start_date": f"{today}T00:00:00",
#             "expanded": "work.locations,work.operators,events.registry",
#             "limit": 200
#         }
#         return "works/scheduled", params