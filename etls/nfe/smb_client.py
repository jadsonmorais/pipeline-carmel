import os
from pathlib import Path
from dotenv import load_dotenv
import smbclient
import smbclient.path

load_dotenv(Path(__file__).parent.parent.parent / 'auth' / 'prod' / '.env')

HOST   = os.getenv('NFE_SMB_HOST', '10.197.0.51')
USER   = os.getenv('NFE_SMB_USER')       # ex: automacao
PASS   = os.getenv('NFE_SMB_PASS')
DOMAIN = os.getenv('NFE_SMB_DOMAIN', '') # ex: MAGNA

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
    Usar como context manager.

    Exemplo:
        with SMBShareClient() as client:
            for hotel, filename, content in client.iter_xml_files():
                ...
    """

    def __enter__(self):
        smbclient.register_session(HOST)
        return self

    def __exit__(self, *args):
        smbclient.reset_connection_cache()

    def _share_path(self, share_name):
        return f'\\\\{HOST}\\{share_name}'

    def list_xml_files(self, share_name):
        """Retorna lista de nomes de arquivo .xml do share."""
        share_path = self._share_path(share_name)
        return [
            entry.name
            for entry in smbclient.scandir(share_path)
            if entry.name.lower().endswith('.xml')
        ]

    def read_file(self, share_name, filename):
        """Lê o conteúdo de um arquivo XML do share. Retorna string."""
        file_path = f'\\\\{HOST}\\{share_name}\\{filename}'
        with smbclient.open_file(file_path, mode='r', encoding='utf-8', errors='replace') as f:
            return f.read()

    def iter_xml_files(self):
        """
        Itera sobre todos os XMLs de todos os hotéis.
        Yield: (hotel, filename, xml_content)
        """
        for hotel, share_name in HOTEL_SHARES.items():
            print(f'[NF-e SMB] Listando {share_name}...')
            try:
                files = self.list_xml_files(share_name)
                print(f'[NF-e SMB]   {len(files)} XMLs encontrados')
                for filename in files:
                    try:
                        content = self.read_file(share_name, filename)
                        yield hotel, filename, content
                    except Exception as e:
                        print(f'[NF-e SMB]   ERRO ao ler {filename}: {e}')
            except Exception as e:
                print(f'[NF-e SMB] ERRO no share {share_name}: {e}')
