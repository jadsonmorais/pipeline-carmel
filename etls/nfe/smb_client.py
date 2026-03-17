import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / 'auth' / 'prod' / '.env')

HOST = os.getenv('NFE_SMB_HOST', '10.197.0.51')

# Mapeamento hotel canônico → nome do share SMB
HOTEL_SHARES = {
    'CUMBUCO': 'Cumbuco',
    'CHARME':  'Charme',
    'MAGNA':   'Magna',
    'TAIBA':   'Taiba',
}


class SMBShareClient:
    """
    Cliente SMB para leitura dos XMLs de NF-e nas pastas compartilhadas.
    Usar como context manager. Usa o cliente SMB nativo do Windows via pathlib.

    Exemplo:
        with SMBShareClient() as client:
            for hotel, filename, content in client.iter_xml_files():
                ...
    """

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def _iter_files(self, pattern, label, skip_ids, skip_suffix=None):
        """Iterador genérico: lista arquivos por padrão, pula já processados."""
        for hotel, share_name in HOTEL_SHARES.items():
            share_path = Path(f'\\\\{HOST}\\{share_name}')
            print(f'[NF-e SMB] [{label}] Listando {share_name}...')
            try:
                all_files = list(share_path.glob(pattern))
                if skip_suffix:
                    all_files = [f for f in all_files if f.name.endswith(skip_suffix)]
                novos = [f for f in all_files if f.stem not in skip_ids]
                print(f'[NF-e SMB]   {len(all_files)} total, {len(novos)} novos')
                for xml_path in novos:
                    try:
                        content = xml_path.read_text(encoding='utf-8', errors='replace')
                        yield hotel, xml_path.name, content
                    except Exception as e:
                        print(f'[NF-e SMB]   ERRO ao ler {xml_path.name}: {e}')
            except Exception as e:
                print(f'[NF-e SMB] ERRO no share {share_name}: {e}')

    def iter_xml_files(self, skip_ids=None):
        """
        Itera sobre XMLs de NF-e/NFC-e novos (arquivos que NÃO terminam em -can.xml).
        Yield: (hotel, filename, xml_content)
        """
        skip_ids = skip_ids or set()
        for hotel, share_name in HOTEL_SHARES.items():
            share_path = Path(f'\\\\{HOST}\\{share_name}')
            print(f'[NF-e SMB] [NF-e] Listando {share_name}...')
            try:
                all_files = [f for f in share_path.glob('*.xml') if not f.name.endswith('-can.xml')]
                novos = [f for f in all_files if f.stem not in skip_ids]
                print(f'[NF-e SMB]   {len(all_files)} total, {len(novos)} novos')
                for xml_path in novos:
                    try:
                        content = xml_path.read_text(encoding='utf-8', errors='replace')
                        yield hotel, xml_path.name, content
                    except Exception as e:
                        print(f'[NF-e SMB]   ERRO ao ler {xml_path.name}: {e}')
            except Exception as e:
                print(f'[NF-e SMB] ERRO no share {share_name}: {e}')

    def iter_cancelamento_files(self, skip_ids=None):
        """
        Itera sobre XMLs de cancelamento novos (arquivos que terminam em -can.xml).
        Yield: (hotel, filename, xml_content)
        """
        yield from self._iter_files('*-can.xml', 'CAN', skip_ids or set())
