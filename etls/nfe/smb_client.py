import os
from pathlib import Path
from dotenv import load_dotenv
from smbprotocol.connection import Connection
from smbprotocol.session import Session
from smbprotocol.tree import TreeConnect
from smbprotocol.open import Open, CreateDisposition, CreateOptions, FileAttributes, FilePipePrinterAccessMask, ShareAccess
from smbprotocol.query_info import SMB2QueryInfoRequest, InfoType, FileInformationClass
import smbprotocol.open as smb_open
import uuid

load_dotenv(Path(__file__).parent.parent.parent / 'auth' / 'prod' / '.env')

HOST = os.getenv('NFE_SMB_HOST', '10.197.0.51')
USER = os.getenv('NFE_SMB_USER')
PASS = os.getenv('NFE_SMB_PASS')
DOMAIN = os.getenv('NFE_SMB_DOMAIN', '')

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

    def __init__(self):
        self._conn = None
        self._session = None

    def __enter__(self):
        self._conn = Connection(uuid.uuid4(), HOST, 445)
        self._conn.connect()
        self._session = Session(self._conn, USER, PASS, require_encryption=False)
        self._session.connect()
        return self

    def __exit__(self, *args):
        if self._session:
            self._session.disconnect()
        if self._conn:
            self._conn.disconnect()

    def list_xml_files(self, share_name):
        """Lista todos os arquivos .xml de um share. Retorna lista de nomes."""
        tree = TreeConnect(self._session, f'\\\\{HOST}\\{share_name}')
        tree.connect()
        try:
            dir_open = Open(tree, '')
            dir_open.create(
                ImpersonationLevel=smb_open.ImpersonationLevel.Impersonation,
                DesiredAccess=FilePipePrinterAccessMask.GENERIC_READ,
                FileAttributes=FileAttributes.FILE_ATTRIBUTE_DIRECTORY,
                ShareAccess=ShareAccess.FILE_SHARE_READ,
                CreateDisposition=CreateDisposition.FILE_OPEN,
                CreateOptions=CreateOptions.FILE_DIRECTORY_FILE,
            )
            entries = dir_open.query_directory('*', FileInformationClass.FileNamesInformation)
            dir_open.close()
            return [
                e['file_name'].get_value().decode('utf-16-le')
                for e in entries
                if e['file_name'].get_value().decode('utf-16-le').lower().endswith('.xml')
            ]
        finally:
            tree.disconnect()

    def read_file(self, share_name, filename):
        """Lê o conteúdo de um arquivo XML do share. Retorna string UTF-8."""
        tree = TreeConnect(self._session, f'\\\\{HOST}\\{share_name}')
        tree.connect()
        try:
            file_open = Open(tree, filename)
            file_open.create(
                ImpersonationLevel=smb_open.ImpersonationLevel.Impersonation,
                DesiredAccess=FilePipePrinterAccessMask.GENERIC_READ,
                FileAttributes=FileAttributes.FILE_ATTRIBUTE_NORMAL,
                ShareAccess=ShareAccess.FILE_SHARE_READ,
                CreateDisposition=CreateDisposition.FILE_OPEN,
                CreateOptions=CreateOptions.FILE_NON_DIRECTORY_FILE,
            )
            raw = b''
            offset = 0
            chunk = 65536
            while True:
                data = file_open.read(offset, chunk)
                if not data:
                    break
                raw += data
                offset += len(data)
                if len(data) < chunk:
                    break
            file_open.close()
            return raw.decode('utf-8', errors='replace')
        finally:
            tree.disconnect()

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
