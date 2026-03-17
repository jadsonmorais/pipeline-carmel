from . import smb_client as smb_module, parser
from shared import db as utils

BATCH_SIZE = 500


def _flush(batch, table, id_col, type_label):
    if batch:
        utils.upsert_raw_data(table, id_col, batch, type_label)
        print(f'[NF-e] {len(batch)} registros persistidos em {table}')
        batch.clear()


def run():
    print('--- NF-e SMB Sync ---')

    existing_nfe_ids = utils.get_existing_nfe_ids()
    existing_can_ids = utils.get_existing_cancelamento_ids()
    print(f'[NF-e] {len(existing_nfe_ids)} NF-es já no banco')
    print(f'[NF-e] {len(existing_can_ids)} cancelamentos já no banco')

    nfe_total = 0
    can_total = 0
    errors = 0

    with smb_module.SMBShareClient() as client:
        batch = []
        for hotel, filename, xml_content in client.iter_xml_files(skip_ids=existing_nfe_ids):
            try:
                batch.append(parser.parse_xml(xml_content, hotel, filename))
                if len(batch) >= BATCH_SIZE:
                    _flush(batch, 'nfe_raw_xmls', 'nota_id', 'nota')
                    nfe_total += BATCH_SIZE
            except Exception as e:
                print(f'[NF-e] ERRO ao parsear {filename}: {e}')
                errors += 1
        nfe_total += len(batch)
        _flush(batch, 'nfe_raw_xmls', 'nota_id', 'nota')

        batch = []
        for hotel, filename, xml_content in client.iter_cancelamento_files(skip_ids=existing_can_ids):
            try:
                batch.append(parser.parse_cancelamento(xml_content, hotel, filename))
                if len(batch) >= BATCH_SIZE:
                    _flush(batch, 'nfe_raw_cancelamentos', 'cancelamento_id', 'cancelamento')
                    can_total += BATCH_SIZE
            except Exception as e:
                print(f'[NF-e] ERRO ao parsear cancelamento {filename}: {e}')
                errors += 1
        can_total += len(batch)
        _flush(batch, 'nfe_raw_cancelamentos', 'cancelamento_id', 'cancelamento')

    print(f'[NF-e] Total: {nfe_total} NF-es, {can_total} cancelamentos persistidos')
    if errors:
        print(f'[NF-e] Erros de parse: {errors}')

    print('--- NF-e SMB Sync finalizado ---')


if __name__ == '__main__':
    run()
