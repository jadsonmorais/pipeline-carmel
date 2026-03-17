import sys
from datetime import date, timedelta

from . import sftp as sftp_module, parser
from shared import db as utils


def run(date_str=None):
    if date_str is None:
        date_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f'--- PDV Sync: {date_str} ---')

    with sftp_module.SFTPClient() as client:
        files = client.list_files_for_date(date_str)

        if not files:
            print(f'[PDV] Nenhum arquivo encontrado para {date_str}')
            return

        all_records = []
        for filename in files:
            print(f'[PDV] Baixando {filename}...')
            content = client.download_content(filename)
            records = parser.parse_file(content, filename)
            print(f'[PDV]   {len(records)} notas em {filename}')
            all_records.extend(records)

    if all_records:
        utils.upsert_raw_data('pdv_raw_notas', 'nota_id', all_records, 'nota')
        print(f'[PDV] Total: {len(all_records)} notas persistidas')

    print('--- PDV Sync finalizado ---')


if __name__ == '__main__':
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(date_arg)
