import requests
import time
from shared import db as utils


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

        # Gerenciamento de Throttling (Limite de 60 req/min)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"Limite atingido. Aguardando {retry_after} segundos...")
            time.sleep(retry_after)
            return self.request(endpoint, params)

        response.raise_for_status()
        return response.json()


    def request_all_pages(self, endpoint, params=None):
        """Gerencia a paginação automaticamente"""
        if params is None: params = {}
        params['page'] = 1
        all_data = []

        while True:
            response = self.request(endpoint, params)
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
        from datetime import datetime
        today = datetime.now().strftime('%Y-%m-%d')
        return "works/scheduled", {
            "date_min_start_date": f"{today}T00:00:00",
            "limit": 200
        }

    @staticmethod
    def get_works_bulk():
        """Captura a estrutura dos modelos de trabalho (Works)"""
        return "works", {
            "expanded": "workPeriodicity,workSlaRules,workType,client,locations",
            "limit": 200
        }

    @staticmethod
    def get_failures_bulk(date_start, date_end=None):
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
        params = {
            "expanded": "operator,location,client,problem",
            "limit": 200
        }
        return "failures/open", params
