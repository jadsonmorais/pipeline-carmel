import json
import os
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / 'auth' / 'prod' / '.env')

BASE_URL      = 'https://carmel-api.cmerp.com.br/Global.API/ExportarDados/Executar'
CLIENT_ID     = os.getenv('CLIENT-ID')
CLIENT_SECRET = os.getenv('CLIENT-SECRET')
EMPRESA_IDS   = os.getenv('EMPRESA-IDS', '1,2,3,4,8')
USUARIO_ID    = os.getenv('USUARIO-ID', '1')
ID_EXPORTACAO = 80


def fetch_lancamentos(date_ini_str, date_fim_str):
    """Busca lançamentos fiscais no intervalo [date_ini, date_fim].
    date_ini_str / date_fim_str: 'YYYY-MM-DD'
    Retorna lista de dicts (resposta JSON).
    """
    parametros = [
        {'Nome': 'DataIni', 'TipoDado': 'DateTime', 'ValorDateTime': f'{date_ini_str}T00:00:00'},
        {'Nome': 'DataFim', 'TipoDado': 'DateTime', 'ValorDateTime': f'{date_fim_str}T23:59:59'},
        {'Nome': 'IdEmpresa', 'TipoDado': 'String', 'ValorString': EMPRESA_IDS},
    ]
    params = {
        'idExportacao': ID_EXPORTACAO,
        'compactarDados': 'false',
        'retornarComoAnexo': 'false',
        'parametros': json.dumps(parametros, ensure_ascii=False),
    }
    headers = {
        'Accept': 'application/json',
        'x-cmflex-client-id': CLIENT_ID,
        'x-cmflex-client-secret': CLIENT_SECRET,
        'x-cmflex-empresaId': '1',
        'x-cmflex-usuarioId': USUARIO_ID,
    }
    resp = requests.get(BASE_URL, params=params, headers=headers, timeout=120)
    resp.raise_for_status()
    return resp.json()
