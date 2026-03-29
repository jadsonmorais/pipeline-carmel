"""
Testes para etls/pdv/parser.py e etls/nfe/parser.py.
Sem dependências externas — usa apenas os arquivos de examples/.
"""

import re
import pytest
from etls.pdv.parser import parse_file, STORE_TO_HOTEL
from etls.nfe.parser import parse_xml, parse_cancelamento


# ── PDV / Simphony ─────────────────────────────────────────────

class TestPdvParser:
    def test_retorna_lista_nao_vazia(self, pdv_charme_bytes):
        records = parse_file(pdv_charme_bytes, "CharmeCFB_2026-02-19.json")
        assert isinstance(records, list)
        assert len(records) > 0

    def test_todo_registro_tem_id_44_digitos(self, pdv_charme_bytes):
        records = parse_file(pdv_charme_bytes, "CharmeCFB_2026-02-19.json")
        for rec in records:
            assert "id" in rec
            assert re.fullmatch(r"\d{44}", rec["id"]), f"id inválido: {rec['id']}"

    def test_hotel_mapeado_para_charme(self, pdv_charme_bytes):
        records = parse_file(pdv_charme_bytes, "CharmeCFB_2026-02-19.json")
        assert all(rec["hotel"] == "CHARME" for rec in records)

    def test_source_file_preservado(self, pdv_charme_bytes):
        filename = "CharmeCFB_2026-02-19.json"
        records = parse_file(pdv_charme_bytes, filename)
        assert all(rec["source_file"] == filename for rec in records)

    def test_idempotencia(self, pdv_charme_bytes):
        r1 = parse_file(pdv_charme_bytes, "CharmeCFB_2026-02-19.json")
        r2 = parse_file(pdv_charme_bytes, "CharmeCFB_2026-02-19.json")
        assert r1 == r2

    def test_hotel_cumbuco(self, pdv_cumbuco_bytes):
        records = parse_file(pdv_cumbuco_bytes, "CumbucoCFB_2026-02-19.json")
        assert len(records) > 0
        assert all(rec["hotel"] == "CUMBUCO" for rec in records)

    def test_mapeamento_store_to_hotel_completo(self):
        assert STORE_TO_HOTEL["CARM"] == "CHARME"
        assert STORE_TO_HOTEL["MAGN"] == "MAGNA"
        assert STORE_TO_HOTEL["TAIBA"] == "TAIBA"
        assert STORE_TO_HOTEL["CUMBUCO"] == "CUMBUCO"


# ── NF-e XML ───────────────────────────────────────────────────

NF_FILENAME = "NFe23260319253187000163650010000170031192531872-nfe.xml"
NOTA_CHAVE = "23260319253187000163650010000170031192531872"


class TestNfeParser:
    def test_retorna_dict(self, nfe_xml_str):
        result = parse_xml(nfe_xml_str, "CHARME", NF_FILENAME)
        assert isinstance(result, dict)

    def test_nota_id_44_digitos(self, nfe_xml_str):
        result = parse_xml(nfe_xml_str, "CHARME", NF_FILENAME)
        nota_id = result.get("id") or result.get("nota_id")
        assert nota_id is not None
        assert re.fullmatch(r"\d{44}", nota_id), f"nota_id inválido: {nota_id}"

    def test_nota_id_sem_prefixo_nfe(self, nfe_xml_str):
        result = parse_xml(nfe_xml_str, "CHARME", NF_FILENAME)
        nota_id = result.get("id") or result.get("nota_id")
        assert not nota_id.startswith("NFe"), "nota_id não deve ter prefixo 'NFe'"

    def test_hotel_preservado(self, nfe_xml_str):
        result = parse_xml(nfe_xml_str, "CHARME", NF_FILENAME)
        assert result["hotel"] == "CHARME"

    def test_source_file_preservado(self, nfe_xml_str):
        result = parse_xml(nfe_xml_str, "CHARME", NF_FILENAME)
        assert result["source_file"] == NF_FILENAME

    def test_xml_invalido_lanca_value_error(self):
        with pytest.raises(ValueError, match="XML inválido"):
            parse_xml("isto não é xml", "CHARME", "fake.xml")


# ── Cancelamento NF-e ─────────────────────────────────────────

class TestCancelamentoParser:
    def test_retorna_dict(self, cancelamento_xml_str):
        result = parse_cancelamento(cancelamento_xml_str, "CHARME",
                                    "ID1101112326032779485200015465001000005981127794852801-can.xml")
        assert isinstance(result, dict)

    def test_tem_chave_cancelamento(self, cancelamento_xml_str):
        result = parse_cancelamento(cancelamento_xml_str, "CHARME",
                                    "ID1101112326032779485200015465001000005981127794852801-can.xml")
        cid = result.get("id") or result.get("cancelamento_id")
        assert cid is not None and len(cid) > 0

    def test_tem_chNFe(self, cancelamento_xml_str):
        result = parse_cancelamento(cancelamento_xml_str, "CHARME",
                                    "ID1101112326032779485200015465001000005981127794852801-can.xml")
        assert "chNFe" in result
