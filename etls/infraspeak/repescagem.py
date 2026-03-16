from . import api
from shared import db as utils
from .extractor import InfraspeakExtractor
import os
import re


def processar_arquivo(arquivo_nome, resource_type, include_records, extractor):
    """Lê o arquivo, limpa a sujeira do SQL e aciona o extrator."""
    if not os.path.exists(arquivo_nome):
        return

    ids_limpos = []

    with open(arquivo_nome, 'r', encoding='utf-8') as f:
        for linha in f:
            id_limpo = re.sub(r'[^0-9]', '', linha)
            if id_limpo:
                ids_limpos.append(id_limpo)

    ids_limpos = list(set(ids_limpos))
    total = len(ids_limpos)

    if total > 0:
        print(f"\n>>> Lendo '{arquivo_nome}': Iniciando repescagem de {total} {resource_type}s...")
        extractor.sync_details(ids_limpos, resource_type, include_records=include_records)
        print(f"Repescagem de {resource_type} concluída!")


def rodar_repescagem_completa():
    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    api_client = api.ApiInfraspeak(user, token)
    extract = InfraspeakExtractor(api_client)

    print("--- Iniciando Operação de Repescagem Múltipla ---")

    processar_arquivo('repescagem_failures.csv', 'failure', True, extract)
    processar_arquivo('repescagem_works.csv', 'work', False, extract)
    processar_arquivo('repescagem_scheduled.csv', 'scheduled_work', True, extract)

    print("\nProcesso finalizado. Todos os arquivos disponíveis foram processados.")


if __name__ == "__main__":
    rodar_repescagem_completa()
