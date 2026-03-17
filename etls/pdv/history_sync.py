import sys
from datetime import datetime, timedelta

from . import sftp as sftp_module, parser
from shared import db as utils


def get_days_list(date_start_str, date_end_str):
    start = datetime.strptime(date_start_str, '%Y-%m-%d')
    end = datetime.strptime(date_end_str, '%Y-%m-%d')
    return [(start + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end - start).days + 1)]


def run_historical_sync(date_start, date_end):
    print(f'--- PDV Histórico: {date_start} até {date_end} ---')

    dias = get_days_list(date_start, date_end)
    total_notas = 0
    dias_sem_arquivo = []

    with sftp_module.SFTPClient() as client:
        for dia in dias:
            print(f'\n>>> {dia}')
            try:
                files = client.list_files_for_date(dia)
                if not files:
                    print(f'  Nenhum arquivo encontrado para {dia}')
                    dias_sem_arquivo.append(dia)
                    continue

                for filename in sorted(files):
                    content = client.download_content(filename)
                    records = parser.parse_file(content, filename)
                    if records:
                        utils.upsert_raw_data('pdv_raw_notas', 'nota_id', records, 'nota')
                        print(f'  {filename}: {len(records)} notas')
                        total_notas += len(records)

            except Exception as e:
                print(f'  ERRO em {dia}: {e}')
                continue

    print(f'\n--- PDV Histórico finalizado ---')
    print(f'    Total de notas persistidas: {total_notas}')
    if dias_sem_arquivo:
        print(f'    Dias sem arquivo ({len(dias_sem_arquivo)}): {", ".join(dias_sem_arquivo)}')


if __name__ == '__main__':
    if len(sys.argv) >= 3:
        run_historical_sync(sys.argv[1], sys.argv[2])
    else:
        print('Uso: python -m etls.pdv.history_sync 2026-02-01 2026-02-28')
