import requests
import json
from datetime import datetime, timedelta
from shared import db as utils
from .api import ApiInfraspeak

# Carregamento de credenciais (já feito ao importar shared.db, mas garantimos aqui)
BASE_URL = "https://api.infraspeak.com/v3"

HEADERS = {
    "Accept": "application/json",
    "Authorization": f"Bearer {utils.os.getenv('API_DATA_TOKEN')}"
}


def test_endpoint_filter(endpoint_path, filter_name, test_date):
    """
    Testa se um endpoint aceita um determinado parâmetro de filtro JQL de data.
    """
    url = f"{BASE_URL}/{endpoint_path}?{filter_name}={test_date}&limit=1"

    try:
        response = requests.get(url, headers=HEADERS)
        status_code = response.status_code

        if status_code == 200:
            data = response.json()
            total_records = data.get("meta", {}).get("pagination", {}).get("total", "Desconhecido")
            print(f" [VÁLIDO] Parâmetro '{filter_name}' funciona em '/{endpoint_path}'. (Registos encontrados: {total_records})")
        elif status_code == 400:
            print(f" [INVÁLIDO] Parâmetro '{filter_name}' NÃO é aceite em '/{endpoint_path}' (Erro 400).")
        else:
            print(f" [AVISO] '/{endpoint_path}' retornou HTTP {status_code} com o filtro '{filter_name}'.")
            print(response.text)

    except Exception as e:
        print(f" [ERRO] Falha na requisição para /{endpoint_path}: {e}")


def main():
    data_teste = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%dT00:00:00")
    print(f"Iniciando validação de JQL com a data base: {data_teste}\n")

    print("--- Testando entidade Mestre: /works ---")
    for filtro in ["date_min_updated_at", "date_min_created_at", "date_min_start_date"]:
        test_endpoint_filter("works", filtro, data_teste)

    print("\n--- Testando entidade Ocorrência: /works/scheduled ---")
    for filtro in ["date_min_updated_at", "date_min_start_date", "date_min_last_status_change_date"]:
        test_endpoint_filter("works/scheduled", filtro, data_teste)


if __name__ == "__main__":
    main()
