import sys
from datetime import date, timedelta

from . import sftp as sftp_module, parser
from shared import db as utils


def run_historical_sync(date_start, date_end):
    start = date.fromisoformat(date_start)
    end = date.fromisoformat(date_end)

    all_dates = []
    current = start
    while current <= end:
        all_dates.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)

    print(f'--- GCM History Sync: {date_start} → {date_end} ({len(all_dates)} dias) ---')

    total_records = 0
    days_without_files = []

    with sftp_module.SFTPClient() as client:
        for date_str in all_dates:
            try:
                files = client.list_files_for_date(date_str)
                if not files:
                    days_without_files.append(date_str)
                    continue

                day_records = []
                for filename in files:
                    content = client.download_content(filename)
                    records = parser.parse_file(content, filename)
                    day_records.extend(records)

                if day_records:
                    utils.upsert_raw_data('gcm_raw_line_items', 'line_item_id', day_records, 'line_item')
                    total_records += len(day_records)
                    print(f'[GCM] {date_str}: {len(day_records)} line items')

            except Exception as e:
                print(f'[GCM] ERRO em {date_str}: {e}')

    print(f'\n[GCM] Concluído: {total_records} line items persistidos.')
    if days_without_files:
        print(f'[GCM] Dias sem arquivo ({len(days_without_files)}): {", ".join(days_without_files)}')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Uso: python -m etls.gcm.history_sync <data_inicio> <data_fim>')
        print('Ex:  python -m etls.gcm.history_sync 2026-01-01 2026-03-26')
        sys.exit(1)
    run_historical_sync(sys.argv[1], sys.argv[2])
