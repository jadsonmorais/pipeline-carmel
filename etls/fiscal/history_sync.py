import sys
from datetime import date, timedelta
from . import api
from shared import db as utils

CHUNK_DIAS = 30


def _to_records(raw_list):
    seen = {}
    for item in raw_list:
        item['id'] = str(item['IDLANCAMENTOICMSBASE'])
        seen[item['id']] = item  # última ocorrência prevalece
    return list(seen.values())


def run(date_ini_str, date_fim_str):
    ini = date.fromisoformat(date_ini_str)
    fim = date.fromisoformat(date_fim_str)
    print(f'--- Fiscal History Sync: {ini} → {fim} ---')
    total = 0
    cursor = ini
    while cursor <= fim:
        chunk_fim = min(cursor + timedelta(days=CHUNK_DIAS - 1), fim)
        print(f'[FISCAL] Chunk: {cursor} → {chunk_fim}')
        try:
            raw = api.fetch_lancamentos(cursor.strftime('%Y-%m-%d'), chunk_fim.strftime('%Y-%m-%d'))
            records = _to_records(raw)
            if records:
                utils.upsert_raw_data('fiscal_raw_lancamentos', 'lancamento_id', records, 'lancamento')
                total += len(records)
                print(f'[FISCAL]   {len(records)} lançamentos persistidos')
        except Exception as e:
            print(f'[FISCAL]   ERRO no chunk {cursor} → {chunk_fim}: {e}')
        cursor = chunk_fim + timedelta(days=1)
    utils.refresh_mv_fiscal()
    print('[FISCAL] mv_fiscal_lancamentos atualizada')
    print(f'--- Fiscal History Sync finalizado: {total} lançamentos no total ---')


if __name__ == '__main__':
    run(sys.argv[1], sys.argv[2])
