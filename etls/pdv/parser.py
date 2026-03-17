import json


# Mapeamento Store Number → nome canônico do hotel
STORE_TO_HOTEL = {
    'CUMBUCO': 'CUMBUCO',
    'TAIBA':   'TAIBA',
    'CARM':    'CHARME',
    'MAGN':    'MAGNA',
}


def parse_file(content, filename):
    """
    Parseia um arquivo JSON do PDV Simphony e retorna lista de registros para upsert.

    Estrutura do arquivo:
      [ [header FIS], [{}], [registros FISID...], [{}] ]

    Retorna lista de dicts com:
      - id: Invoice Data Info 8 (chave NF-e 44 dígitos) — PK e link com SEFAZ
      - hotel: nome canônico derivado do Store Number do header
      - source_file: nome do arquivo de origem
      - demais campos do registro FISID
    """
    data = json.loads(content)

    # Seção 0: header FIS
    header = data[0][0] if data and data[0] else {}
    store_number = header.get('Store Number', '')
    hotel = STORE_TO_HOTEL.get(store_number, store_number)

    # Seção 2: registros FISID
    fisid_section = data[2] if len(data) > 2 else []

    records = []
    for rec in fisid_section:
        if not isinstance(rec, dict):
            continue
        chave_nfe = (rec.get('Invoice Data Info 8') or '').strip()
        if not chave_nfe:
            continue
        records.append({
            'id': chave_nfe,
            'hotel': hotel,
            'source_file': filename,
            **rec,
        })

    return records
