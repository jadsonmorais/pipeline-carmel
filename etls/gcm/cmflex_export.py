"""
Gerador de JSON CMFlex para importação de vendas de Consumo Interno.

Uso:
    python -m etls.gcm.cmflex_export 2026-03-26                     # todos os hotéis
    python -m etls.gcm.cmflex_export 2026-03-26 "CARMEL CHARME RESORT"  # só Charme

Gera um arquivo por hotel em output/{hotel}_cmflex_{data}.json

Variáveis de ambiente por hotel (chave = locationName em maiúsculas com espaços → _):
    GCM_ECF_SERIAL_CARMEL_CHARME_RESORT=CHARME.SERVIDOR-CAPS
    GCM_ECF_SERIAL_CARMEL_CUMBUCO_WIND_RESORT=WIND.SRV-CAPS
    GCM_ECF_SERIAL_CARMEL_TAIBA_EXCLUSIVE_RESORT=TAIBA.TAIBA-CIPO
    GCM_ECF_SERIAL_MAGNA_PRAIA_HOTEL=MAGNA.SRV-CAPS

    GCM_EMPRESA_ID_CARMEL_CHARME_RESORT=6
    GCM_EMPRESA_ID_CARMEL_CUMBUCO_WIND_RESORT=...
    GCM_EMPRESA_ID_CARMEL_TAIBA_EXCLUSIVE_RESORT=...
    GCM_EMPRESA_ID_MAGNA_PRAIA_HOTEL=...

    GCM_CODIGO_EMPRESA_CARMEL_CHARME_RESORT=POS003
    GCM_CODIGO_EMPRESA_CARMEL_CUMBUCO_WIND_RESORT=...
    GCM_CODIGO_EMPRESA_CARMEL_TAIBA_EXCLUSIVE_RESORT=...
    GCM_CODIGO_EMPRESA_MAGNA_PRAIA_HOTEL=...

    GCM_CHAVE_ACESSO_CARMEL_CHARME_RESORT=ec8f94e8-...
    GCM_CHAVE_ACESSO_CARMEL_CUMBUCO_WIND_RESORT=...
    GCM_CHAVE_ACESSO_CARMEL_TAIBA_EXCLUSIVE_RESORT=...
    GCM_CHAVE_ACESSO_MAGNA_PRAIA_HOTEL=...
"""
import json
import os
import sys
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / 'auth' / 'prod' / '.env')

# Mapeamento locationName → nome canônico do hotel (para nomear o arquivo de saída)
LOCATION_NAME_TO_HOTEL = {
    'CARMEL CHARME RESORT':              'CHARME',
    'CARMEL CUMBUCO WIND RESORT':        'CUMBUCO',
    'CARMEL TAIBA EXCLUSIVE RESORT':     'TAIBA',
    'MAGNA PRAIA HOTEL':                 'MAGNA',
}


def _env_key(prefix, location_name):
    sanitized = location_name.upper().replace(' ', '_')
    return f'{prefix}_{sanitized}'


def _get_ecf_serial(location_name):
    return os.getenv(_env_key('GCM_ECF_SERIAL', location_name), '')


def _get_empresa_id(location_name):
    val = os.getenv(_env_key('GCM_EMPRESA_ID', location_name), '0')
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _get_codigo_empresa(location_name):
    return os.getenv(_env_key('GCM_CODIGO_EMPRESA', location_name), '')


def _get_chave_acesso(location_name):
    return os.getenv(_env_key('GCM_CHAVE_ACESSO', location_name), '')


def _get_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        port=int(os.getenv('DB_PORT', '5432')),
    )


def _fetch_consumo_interno(date_str, location_name=None):
    """Retorna line items de Consumo Interno para a data, opcionalmente filtrado por locationName."""
    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            if location_name:
                cur.execute("""
                    SELECT data FROM carmel.gcm_raw_line_items
                    WHERE data->>'businessDate' = %s
                      AND data->>'orderTypeName' = 'Consumo Interno'
                      AND (data->>'lineTotal')::numeric <> 0
                      AND data->>'locationName' = %s
                    ORDER BY (data->>'locationName'), (data->>'guestCheckID')::bigint, (data->>'lineNum')::int
                """, (date_str, location_name))
            else:
                cur.execute("""
                    SELECT data FROM carmel.gcm_raw_line_items
                    WHERE data->>'businessDate' = %s
                      AND data->>'orderTypeName' = 'Consumo Interno'
                      AND (data->>'lineTotal')::numeric <> 0
                    ORDER BY (data->>'locationName'), (data->>'guestCheckID')::bigint, (data->>'lineNum')::int
                """, (date_str,))
            return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def _build_venda(item, location_name):
    """Constrói um objeto Venda para um line item."""
    line_total = float(item.get('lineTotal') or 0)
    line_count = item.get('lineCount') or 1
    unit_price = line_total / line_count if line_count else 0

    return {
        'CodigoPDV': str(item.get('revenueCenterNum', '')),
        'EmissordoCupom': _get_ecf_serial(location_name),
        'CodigoExterno': str(item.get('menuItemNum', '')),
        'UnidadeMedida': 'UN',
        'Quantidade': float(line_count),
        'ValorUnitario': round(unit_price, 2),
    }


def generate(date_str, location_name=None):
    """Gera JSONs CMFlex para todos os hotéis (ou só o informado) na data."""
    items = _fetch_consumo_interno(date_str, location_name)

    if not items:
        print(f'[GCM CMFlex] Nenhum item de Consumo Interno encontrado para {date_str}')
        return

    # Agrupar por locationName → [line items]
    by_location = {}
    for item in items:
        loc = item.get('locationName', 'UNKNOWN')
        by_location.setdefault(loc, []).append(item)

    output_dir = Path(__file__).parent.parent.parent / 'output'
    output_dir.mkdir(exist_ok=True)

    for loc, loc_items in by_location.items():
        hotel_name = LOCATION_NAME_TO_HOTEL.get(loc, loc.replace(' ', '_'))

        payload = {
            'EmpresaId': _get_empresa_id(loc),
            'TipoDeProcessamento': 'ProcVendaParaBaixaEstoque',
            'ExecucaoImediata': False,
            'NomeParaArmazenamentoDoConteudo': 'AlmoxVendaParaBaixaEstoque',
            'ConteudoCompactado': False,
            'MensagemDoSolicitante': 'IncluirProcVendaParaBaixaEstoque',
            'DataDoMovimento': date_str,
            'CodigoDaEmpresa': _get_codigo_empresa(loc),
            'ChaveDeAcesso': _get_chave_acesso(loc),
            'Vendas': {
                'Venda': [_build_venda(item, loc) for item in loc_items],
            },
        }

        out_path = output_dir / f'{hotel_name}_cmflex_{date_str}.json'
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent='\t')

        print(f'[GCM CMFlex] {hotel_name}: {len(loc_items)} itens → {out_path}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python -m etls.gcm.cmflex_export <data> [locationName]')
        print('Ex:  python -m etls.gcm.cmflex_export 2026-03-26')
        print('Ex:  python -m etls.gcm.cmflex_export 2026-03-26 "CARMEL CHARME RESORT"')
        sys.exit(1)

    date_arg = sys.argv[1]
    loc_arg = sys.argv[2] if len(sys.argv) > 2 else None
    generate(date_arg, loc_arg)
