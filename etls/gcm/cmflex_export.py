"""
Gerador de XML CMFlex para importação de vendas de Consumo Interno.

Uso:
    python -m etls.gcm.cmflex_export 2026-03-26                     # todos os hotéis
    python -m etls.gcm.cmflex_export 2026-03-26 "CARMEL CHARME RESORT"  # só Charme

Gera um arquivo por hotel em output/{hotel}_cmflex_{data}.xml

Variáveis de ambiente por hotel (chave = locationName em maiúsculas com espaços → _):
    GCM_ECF_SERIAL_CARMEL_CHARME_RESORT
    GCM_ECF_SERIAL_CARMEL_CUMBUCO_WIND_RESORT
    GCM_ECF_SERIAL_CARMEL_TAIBA_EXCLUSIVE_RESORT
    GCM_ECF_SERIAL_MAGNA_PRAIA_HOTEL

    GCM_CNPJ_CARMEL_CHARME_RESORT
    GCM_CNPJ_CARMEL_CUMBUCO_WIND_RESORT
    GCM_CNPJ_CARMEL_TAIBA_EXCLUSIVE_RESORT
    GCM_CNPJ_MAGNA_PRAIA_HOTEL

    GCM_ADQUIRENTE_CARMEL_CHARME_RESORT
    GCM_ADQUIRENTE_CARMEL_CUMBUCO_WIND_RESORT
    GCM_ADQUIRENTE_CARMEL_TAIBA_EXCLUSIVE_RESORT
    GCM_ADQUIRENTE_MAGNA_PRAIA_HOTEL
"""
import os
import sys
from collections import defaultdict
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, ElementTree, indent

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


def _ecf_env_key(location_name):
    """Converte locationName em nome de variável de ambiente. Ex: 'CARMEL CHARME RESORT' → 'GCM_ECF_SERIAL_CARMEL_CHARME_RESORT'."""
    sanitized = location_name.upper().replace(' ', '_')
    return f'GCM_ECF_SERIAL_{sanitized}'


def _get_ecf_serial(location_name):
    env_key = _ecf_env_key(location_name)
    return os.getenv(env_key, f'ECF_{location_name.replace(" ", "_")}')


def _get_cnpj(location_name):
    sanitized = location_name.upper().replace(' ', '_')
    return os.getenv(f'GCM_CNPJ_{sanitized}', '')


def _get_adquirente(location_name):
    sanitized = location_name.upper().replace(' ', '_')
    return os.getenv(f'GCM_ADQUIRENTE_{sanitized}', '')


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
                    ORDER BY (data->>'guestCheckID')::bigint, (data->>'lineNum')::int
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


def _format_datetime(dt_str):
    """Converte '2026-03-26T07:30:09' → '2026-03-26T07:30:09-03:00'."""
    if not dt_str:
        return dt_str
    if '+' in dt_str or (dt_str.count('-') > 2 and 'T' in dt_str):
        return dt_str
    return f'{dt_str}-03:00'


def _format_value(v):
    """Formata valor numérico como string sem notação científica."""
    if v is None:
        return '0'
    try:
        f = float(v)
        if f == int(f):
            return str(int(f))
        return f'{f:.2f}'
    except (ValueError, TypeError):
        return str(v)


def _build_pdv_venda(check_items, location_name):
    """Constrói o elemento <PDVVenda> para uma comanda."""
    venda = Element('PDVVenda')

    first = check_items[0]
    check_num = str(first.get('checkNum', 0)).zfill(6)
    dt_emissao = _format_datetime(first.get('transactionDateTime', ''))
    ecf_serial = _get_ecf_serial(location_name)

    total = sum(float(item.get('lineTotal') or 0) for item in check_items)
    all_void = all(item.get('isVoidFlag') == 1 for item in check_items)

    SubElement(venda, 'NumeroDeSerieECF').text = ecf_serial
    SubElement(venda, 'NomeAdquirente').text = _get_adquirente(location_name)
    SubElement(venda, 'CPFCNPJAdquirente').text = _get_cnpj(location_name)
    SubElement(venda, 'NumeroCOO').text = check_num
    SubElement(venda, 'DataEmissao').text = dt_emissao
    SubElement(venda, 'ValorTotal').text = _format_value(total)
    SubElement(venda, 'ValorDesconto').text = '0'
    SubElement(venda, 'ValorAcrescimo').text = '0'
    SubElement(venda, 'Cancelado').text = 'true' if all_void else 'false'
    SubElement(venda, 'IndicadorTipoAcrescimo').text = 'Valor'
    SubElement(venda, 'IndicadorTipoDesconto').text = 'Valor'

    itens_el = SubElement(venda, 'Itens')
    for item in check_items:
        item_el = SubElement(itens_el, 'Item')
        qty = item.get('lineCount') or 1
        line_total = float(item.get('lineTotal') or 0)
        unit_price = line_total / qty if qty else 0
        is_void = item.get('isVoidFlag') == 1
        major_group_num = str(item.get('majorGroupNum') or 0).zfill(3)

        SubElement(item_el, 'CodigoProduto').text = str(item.get('menuItemNum', ''))
        SubElement(item_el, 'DescricaoProduto').text = str(item.get('menuItemName1', ''))
        SubElement(item_el, 'Quantidade').text = _format_value(qty)
        SubElement(item_el, 'Unidade').text = 'UN'
        SubElement(item_el, 'ValorUnitario').text = _format_value(unit_price)
        SubElement(item_el, 'ValorDesconto').text = '0'
        SubElement(item_el, 'ValorAcrescimo').text = '0'
        SubElement(item_el, 'ValorTotalLiquido').text = _format_value(line_total)
        SubElement(item_el, 'TotalizadorParcial').text = major_group_num
        SubElement(item_el, 'Cancelado').text = 'true' if is_void else 'false'
        SubElement(item_el, 'QuantidadeCancelada').text = '0'
        SubElement(item_el, 'ValorCancelado').text = '0'
        SubElement(item_el, 'IndicadorArredTruc').text = 'Arredondamento'

    meios_el = SubElement(venda, 'Meios')
    meio_el = SubElement(meios_el, 'MeioDePagamento')
    SubElement(meio_el, 'NumeroSequencial').text = '0'
    SubElement(meio_el, 'Descricao').text = 'Consumo Interno'
    SubElement(meio_el, 'ValorPago').text = _format_value(total)
    SubElement(meio_el, 'ValorEstornado').text = '0'
    SubElement(meio_el, 'IndicadorEstorno').text = 'Nao'

    return venda


def generate(date_str, location_name=None):
    """Gera XMLs CMFlex para todos os hotéis (ou só o informado) na data."""
    items = _fetch_consumo_interno(date_str, location_name)

    if not items:
        print(f'[GCM CMFlex] Nenhum item de Consumo Interno encontrado para {date_str}')
        return

    # Agrupar por locationName → guestCheckID → [line items]
    by_location = defaultdict(lambda: defaultdict(list))
    for item in items:
        loc = item.get('locationName', 'UNKNOWN')
        check_id = item.get('guestCheckID')
        by_location[loc][check_id].append(item)

    output_dir = Path(__file__).parent.parent.parent / 'output'
    output_dir.mkdir(exist_ok=True)

    for loc, checks in by_location.items():
        hotel_name = LOCATION_NAME_TO_HOTEL.get(loc, loc.replace(' ', '_'))
        root = Element('Vendas')

        for check_id, check_items in checks.items():
            venda = _build_pdv_venda(check_items, loc)
            root.append(venda)

        indent(root, space='\t')
        tree = ElementTree(root)

        out_path = output_dir / f'{hotel_name}_cmflex_{date_str}.xml'
        with open(out_path, 'wb') as f:
            f.write(b'<?xml version="1.0" encoding="utf-8"?>\n')
            tree.write(f, encoding='utf-8', xml_declaration=False)

        n_checks = len(checks)
        n_items = sum(len(v) for v in checks.values())
        print(f'[GCM CMFlex] {hotel_name}: {n_checks} comandas, {n_items} itens → {out_path}')


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Uso: python -m etls.gcm.cmflex_export <data> [locationName]')
        print('Ex:  python -m etls.gcm.cmflex_export 2026-03-26')
        print('Ex:  python -m etls.gcm.cmflex_export 2026-03-26 "CARMEL CHARME RESORT"')
        sys.exit(1)

    date_arg = sys.argv[1]
    loc_arg = sys.argv[2] if len(sys.argv) > 2 else None
    generate(date_arg, loc_arg)
