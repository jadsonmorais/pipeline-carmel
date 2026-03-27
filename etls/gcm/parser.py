import json

LOCATION_TO_HOTEL = {
    'CARM': 'CHARME',
    'CUMBUCO': 'CUMBUCO',
    'TAIBA': 'TAIBA',
    'MAGN': 'MAGNA',
}


def parse_file(content, filename):
    """
    Faz o parse de um arquivo GCM do Simphony.

    Estrutura do JSON: [[record1, record2, ...]]
    Os registros de line items estão em data[0].

    Retorna lista de dicts prontos para upsert, com campo 'id' = guestCheckLineItemID.
    """
    data = json.loads(content)
    records = data[0]

    result = []
    for rec in records:
        line_item_id = rec.get('guestCheckLineItemID')
        if not line_item_id:
            continue

        location_ref = rec.get('locationRef', '')
        hotel = LOCATION_TO_HOTEL.get(location_ref, location_ref)

        result.append({
            'id': line_item_id,
            'hotel': hotel,
            'source_file': filename,
            **rec,
        })

    return result
