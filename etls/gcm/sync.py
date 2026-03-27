import sys
from datetime import date, timedelta

from . import sftp as sftp_module, parser
from shared import db as utils


def run(date_str=None):
    if date_str is None:
        date_str = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    print(f'--- GCM Sync: {date_str} ---')

    with sftp_module.SFTPClient() as client:
        files = client.list_files_for_date(date_str)

        if not files:
            print(f'[GCM] Nenhum arquivo encontrado para {date_str}')
            return

        all_records = []
        for filename in files:
            print(f'[GCM] Baixando {filename}...')
            content = client.download_content(filename)
            records = parser.parse_file(content, filename)
            print(f'[GCM]   {len(records)} line items em {filename}')
            all_records.extend(records)

    if all_records:
        utils.upsert_raw_data('gcm_raw_line_items', 'line_item_id', all_records, 'line_item')
        print(f'[GCM] Total: {len(all_records)} line items persistidos.')


if __name__ == '__main__':
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    run(date_arg)
