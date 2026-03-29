from pathlib import Path
import pytest

EXAMPLES = Path(__file__).parent.parent / "examples"


@pytest.fixture
def pdv_charme_bytes():
    return (EXAMPLES / "CharmeCFB_2026-02-19.json").read_bytes()


@pytest.fixture
def pdv_cumbuco_bytes():
    return (EXAMPLES / "CumbucoCFB_2026-02-19.json").read_bytes()


@pytest.fixture
def nfe_xml_str():
    return (EXAMPLES / "NFe23260319253187000163650010000170031192531872-nfe.xml").read_text(encoding="utf-8")


@pytest.fixture
def cancelamento_xml_str():
    return (EXAMPLES / "ID1101112326032779485200015465001000005981127794852801-can.xml").read_text(
        encoding="utf-8", errors="ignore"
    )
