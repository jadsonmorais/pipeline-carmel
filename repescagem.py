import api
import utils
from extractor import InfraspeakExtractor
import os
import re # Biblioteca nativa do Python para Expressões Regulares

utils.load_dotenv(utils.FILE_AUTH)

def rodar_repescagem_arquivo():
    user = os.getenv('API_DATA_USER')
    token = os.getenv('API_DATA_TOKEN')
    api_client = api.ApiInfraspeak(user, token)
    extract = InfraspeakExtractor(api_client)

    # Você pode colocar 'ids_pendentes.csv' ou 'ids_pendentes.txt' aqui
    arquivo_ids = 'itens_pendentes_P.csv' 
    
    if not os.path.exists(arquivo_ids):
        print(f"❌ Arquivo {arquivo_ids} não encontrado na pasta!")
        return

    ids_scheduled_perdidos = []
    
    # Lê o arquivo e limpa a sujeira visual
    with open(arquivo_ids, 'r', encoding='utf-8') as f:
        for linha in f:
            # O comando abaixo arranca TUDO que não for número (0 a 9)
            id_limpo = re.sub(r'[^0-9]', '', linha)
            
            # Se sobrar algum número (ignora cabeçalhos vazios de números), adiciona na lista
            if id_limpo:
                ids_scheduled_perdidos.append(id_limpo)

    # Remove duplicatas (caso o DBeaver tenha exportado repetido)
    ids_scheduled_perdidos = list(set(ids_scheduled_perdidos))
    
    total = len(ids_scheduled_perdidos)
    print(f"Iniciando repescagem cirúrgica de {total} Scheduled Works perdidos...")

    if total > 0:
        # Envia a lista limpa para o extrator
        extract.sync_details(ids_scheduled_perdidos, 'scheduled_work', include_records=True)
        print("\n✅ Repescagem concluída com sucesso! Todos os buracos foram fechados.")

if __name__ == "__main__":
    rodar_repescagem_arquivo()