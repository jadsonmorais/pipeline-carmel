from . import smb_client as smb_module, parser
from shared import db as utils


def run():
    print('--- NF-e SMB Sync ---')

    existing_nfe_ids = utils.get_existing_nfe_ids()
    existing_can_ids = utils.get_existing_cancelamento_ids()
    print(f'[NF-e] {len(existing_nfe_ids)} NF-es já no banco')
    print(f'[NF-e] {len(existing_can_ids)} cancelamentos já no banco')

    nfe_records = []
    can_records = []
    errors = 0

    with smb_module.SMBShareClient() as client:
        for hotel, filename, xml_content in client.iter_xml_files(skip_ids=existing_nfe_ids):
            try:
                nfe_records.append(parser.parse_xml(xml_content, hotel, filename))
            except Exception as e:
                print(f'[NF-e] ERRO ao parsear {filename}: {e}')
                errors += 1

        for hotel, filename, xml_content in client.iter_cancelamento_files(skip_ids=existing_can_ids):
            try:
                can_records.append(parser.parse_cancelamento(xml_content, hotel, filename))
            except Exception as e:
                print(f'[NF-e] ERRO ao parsear cancelamento {filename}: {e}')
                errors += 1

    if nfe_records:
        utils.upsert_raw_data('nfe_raw_xmls', 'nota_id', nfe_records, 'nota')
        print(f'[NF-e] {len(nfe_records)} NF-es persistidas')
    else:
        print('[NF-e] Nenhuma NF-e nova')

    if can_records:
        utils.upsert_raw_data('nfe_raw_cancelamentos', 'cancelamento_id', can_records, 'cancelamento')
        print(f'[NF-e] {len(can_records)} cancelamentos persistidos')
    else:
        print('[NF-e] Nenhum cancelamento novo')

    if errors:
        print(f'[NF-e] Erros de parse: {errors}')

    print('--- NF-e SMB Sync finalizado ---')


if __name__ == '__main__':
    run()
