import json
import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

FILE_AUTH_PROD = Path(__file__).parent.parent / 'auth' / 'prod' / '.env'
FILE_AUTH_TEST = Path(__file__).parent.parent / 'auth' / 'test' / '.env'
FILE_AUTH = FILE_AUTH_PROD
load_dotenv(FILE_AUTH)


def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        port=os.getenv('DB_PORT')
    )


def upsert_raw_data(table_name, id_column, data_list, type):
    """Realiza o Upsert no Postgres para persistência de dados brutos."""
    conn = get_db_connection()
    cur = conn.cursor()

    insert_data = [(item['id'], json.dumps(item)) for item in data_list]

    query = f"""
        INSERT INTO carmel.{table_name} ({type}_id, data)
        VALUES %s
        ON CONFLICT ({type}_id) DO UPDATE
        SET data = EXCLUDED.data,
            extracted_at = CURRENT_TIMESTAMP;
    """

    execute_values(cur, query, insert_data)
    conn.commit()
    cur.close()
    conn.close()


def get_failure_ids_by_state(state):
    """Retorna lista de failure_ids com determinado state no banco."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT failure_id
        FROM carmel.infraspeak_raw_failures
        WHERE data -> 'attributes' ->> 'state' = %s;
    """, (state,))
    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return ids


def get_existing_cancelamento_ids():
    """Retorna conjunto de cancelamento_ids já persistidos em nfe_raw_cancelamentos."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT cancelamento_id FROM carmel.nfe_raw_cancelamentos;")
    ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return ids


def get_existing_cancelamento_chaves():
    """Retorna conjunto de source_file (nome do arquivo -can.xml) já persistidos.
    Comparação direta com f.name no smb_client, evitando ambiguidade de formato do chNFe."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT data->>'source_file' FROM carmel.nfe_raw_cancelamentos;")
    ids = {row[0] for row in cur.fetchall() if row[0]}
    cur.close()
    conn.close()
    return ids


def get_existing_nfe_ids():
    """Retorna conjunto de nota_ids já persistidos em nfe_raw_xmls."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT nota_id FROM carmel.nfe_raw_xmls;")
    ids = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return ids


def mark_failure_as_deleted(failure_id):
    """Atualiza o state do failure para EXCLUIDO nas tabelas raw e de detalhes quando a API retornar 404."""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE carmel.infraspeak_raw_failures
        SET data = jsonb_set(data, '{attributes,state}', '"EXCLUIDO"'),
            extracted_at = CURRENT_TIMESTAMP
        WHERE failure_id = %s;
    """, (str(failure_id),))
    cur.execute("""
        UPDATE carmel.infraspeak_raw_failure_details
        SET data = jsonb_set(data, '{data,attributes,status}', '"EXCLUIDO"'),
            extracted_at = CURRENT_TIMESTAMP
        WHERE failure_id = %s;
    """, (str(failure_id),))
    conn.commit()
    cur.close()
    conn.close()
