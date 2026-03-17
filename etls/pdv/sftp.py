import os
import paramiko
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent.parent / 'auth' / 'prod' / '.env')

HOST = os.getenv('PDV_SFTP_HOST')
PORT = int(os.getenv('PDV_SFTP_PORT', '22'))
USER = os.getenv('PDV_SFTP_USER')
PASS = os.getenv('PDV_SFTP_PASS')
PATH = os.getenv('PDV_SFTP_PATH', '/d01/carmel_sftp/arquivos')


class SFTPClient:
    """Cliente SFTP para o servidor PDV. Usar como context manager."""

    def __init__(self):
        self._ssh = paramiko.SSHClient()
        self._ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._sftp = None

    def __enter__(self):
        self._ssh.connect(HOST, port=PORT, username=USER, password=PASS)
        self._sftp = self._ssh.open_sftp()
        return self

    def __exit__(self, *args):
        if self._sftp:
            self._sftp.close()
        self._ssh.close()

    def list_files_for_date(self, date_str):
        """Retorna lista de nomes de arquivo JSON que correspondem à data informada."""
        all_files = self._sftp.listdir(PATH)
        return [f for f in all_files if date_str in f and f.endswith('.json')]

    def download_content(self, filename):
        """Baixa o conteúdo de um arquivo da pasta PDV e retorna como string."""
        remote_path = f'{PATH}/{filename}'
        with self._sftp.open(remote_path, 'r') as f:
            return f.read().decode('utf-8')
