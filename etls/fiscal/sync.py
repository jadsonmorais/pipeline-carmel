import sys
from datetime import date, timedelta
from . import api
from shared import db as utils

JANELA_DIAS = 7


def _to_records(raw_list):
    records = []
    for item in raw_list:
        item['id'] = str(item['IDLANCAMENTOICMSBASE'])
        records.append(item)
    return records


def run(date_ini_str=None, date_fim_str=None):
    if date_fim_str is None:
        date_fim_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    if date_ini_str is None:
        date_ini_str = (date.today() - timedelta(days=JANELA_DIAS)).strftime('%Y-%m-%d')

    print(f'--- Fiscal Sync: {date_ini_str} → {date_fim_str} ---')
    raw = api.fetch_lancamentos(date_ini_str, date_fim_str)
    records = _to_records(raw)
    print(f'[FISCAL] {len(records)} lançamentos recebidos')
    if records:
        utils.upsert_raw_data('fiscal_raw_lancamentos', 'lancamento_id', records, 'lancamento')
        print(f'[FISCAL] {len(records)} lançamentos persistidos')
    print('--- Fiscal Sync finalizado ---')


if __name__ == '__main__':
    ini = sys.argv[1] if len(sys.argv) > 1 else None
    fim = sys.argv[2] if len(sys.argv) > 2 else None
    run(ini, fim)
