from . import smb_client as smb_module, parser
from shared import db as utils


def run():
    print('--- NF-e SMB Sync ---')

    records = []
    errors = 0

    with smb_module.SMBShareClient() as client:
        for hotel, filename, xml_content in client.iter_xml_files():
            try:
                record = parser.parse_xml(xml_content, hotel, filename)
                records.append(record)
            except Exception as e:
                print(f'[NF-e] ERRO ao parsear {filename}: {e}')
                errors += 1

    if records:
        utils.upsert_raw_data('nfe_raw_xmls', 'nota_id', records, 'nota')
        print(f'[NF-e] Total: {len(records)} XMLs persistidos')

    if errors:
        print(f'[NF-e] Erros de parse: {errors}')

    print('--- NF-e SMB Sync finalizado ---')


if __name__ == '__main__':
    run()
