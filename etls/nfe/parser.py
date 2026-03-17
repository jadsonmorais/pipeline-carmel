import re
import xml.etree.ElementTree as ET

NS = 'http://www.portalfiscal.inf.br/nfe'


def _tag(name):
    return f'{{{NS}}}{name}'


def _find_text(element, path):
    """Retorna o texto de um elemento ou None se não encontrado."""
    node = element.find(path, {'ns': NS})
    if node is None:
        # Tenta sem namespace explícito (fallback)
        parts = path.split('/')
        node = element
        for part in parts:
            if node is None:
                break
            node = node.find(_tag(part))
    return node.text.strip() if node is not None and node.text else None


def _chave_from_filename(filename):
    """Extrai os 44 dígitos da chave NF-e do nome do arquivo."""
    match = re.search(r'(\d{44})', filename)
    return match.group(1) if match else None


def parse_cancelamento(xml_content, hotel, filename):
    """
    Parseia um XML de evento de cancelamento (envEvento/tpEvento=110111).

    Campos extraídos:
      id / cancelamento_id : Id do infEvento (PK)
      chNFe                : chave 44 dígitos da nota cancelada (FK para nfe_raw_xmls)
      hotel                : nome canônico do hotel
      source_file          : nome do arquivo
      dhEvento             : data/hora do cancelamento
      tpEvento             : 110111
      nSeqEvento           : sequência do evento
      cnpj                 : CNPJ do emitente
      nProt                : protocolo do cancelamento
      xJust                : justificativa
      xml_content          : XML completo
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise ValueError(f'XML inválido em {filename}: {e}')

    evento = root.find(_tag('evento'))
    if evento is None:
        raise ValueError(f'Elemento evento não encontrado em {filename}')

    inf_evento = evento.find(_tag('infEvento'))
    if inf_evento is None:
        raise ValueError(f'Elemento infEvento não encontrado em {filename}')

    def text(node, tag):
        if node is None:
            return None
        child = node.find(_tag(tag))
        return child.text.strip() if child is not None and child.text else None

    event_id = inf_evento.get('Id', '')
    det_evento = inf_evento.find(_tag('detEvento'))

    return {
        'id':               event_id,
        'cancelamento_id':  event_id,
        'chNFe':            text(inf_evento, 'chNFe'),
        'hotel':            hotel,
        'source_file':      filename,
        'dhEvento':         text(inf_evento, 'dhEvento'),
        'tpEvento':         text(inf_evento, 'tpEvento'),
        'nSeqEvento':       text(inf_evento, 'nSeqEvento'),
        'cnpj':             text(inf_evento, 'CNPJ'),
        'nProt':            text(det_evento, 'nProt'),
        'xJust':            text(det_evento, 'xJust'),
        'xml_content':      xml_content,
    }


def parse_xml(xml_content, hotel, filename):
    """
    Parseia um XML NF-e/NFC-e e retorna um dict pronto para upsert.

    Campos extraídos:
      id / nota_id : chave 44 dígitos (PK e chave de conciliação com PDV)
      hotel        : nome canônico do hotel
      source_file  : nome do arquivo de origem
      dhEmi        : data/hora de emissão
      nNF          : número da nota
      serie        : série
      mod          : modelo (55=NF-e, 65=NFC-e)
      tpAmb        : ambiente (1=produção, 2=homologação)
      cnpj_emit    : CNPJ do emitente
      nome_emit    : razão social do emitente
      vNF          : valor total da nota
      nProt        : número do protocolo de autorização
      cStat        : código de status (100 = autorizada)
      dhRecbto     : data/hora do recebimento na SEFAZ
      xml_content  : XML completo (para auditoria e reprocessamento)
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        raise ValueError(f'XML inválido em {filename}: {e}')

    # Suporta tanto <NFe> avulso quanto <nfeProc> com NFe dentro
    nfe_node = root.find(_tag('NFe')) or root
    inf_nfe = nfe_node.find(_tag('infNFe'))
    if inf_nfe is None:
        raise ValueError(f'Elemento infNFe não encontrado em {filename}')

    # Chave: atributo Id do infNFe sem o prefixo "NFe"
    raw_id = inf_nfe.get('Id', '')
    chave = raw_id.replace('NFe', '').replace('NFe', '') if raw_id else _chave_from_filename(filename)
    if not chave or len(chave) != 44:
        raise ValueError(f'Chave NF-e inválida ({chave!r}) em {filename}')

    ide = inf_nfe.find(_tag('ide'))
    emit = inf_nfe.find(_tag('emit'))
    total = inf_nfe.find(_tag('total'))
    icms_tot = total.find(_tag('ICMSTot')) if total is not None else None

    # protNFe (presente no nfeProc)
    prot_nfe = root.find(_tag('protNFe'))
    inf_prot = prot_nfe.find(_tag('infProt')) if prot_nfe is not None else None

    def text(node, tag):
        if node is None:
            return None
        child = node.find(_tag(tag))
        return child.text.strip() if child is not None and child.text else None

    return {
        'id':          chave,
        'nota_id':     chave,
        'hotel':       hotel,
        'source_file': filename,
        'dhEmi':       text(ide, 'dhEmi'),
        'nNF':         text(ide, 'nNF'),
        'serie':       text(ide, 'serie'),
        'mod':         text(ide, 'mod'),
        'tpAmb':       text(ide, 'tpAmb'),
        'cnpj_emit':   text(emit, 'CNPJ'),
        'nome_emit':   text(emit, 'xNome'),
        'vNF':         text(icms_tot, 'vNF'),
        'nProt':       text(inf_prot, 'nProt'),
        'cStat':       text(inf_prot, 'cStat'),
        'dhRecbto':    text(inf_prot, 'dhRecbto'),
        'xml_content': xml_content,
    }
