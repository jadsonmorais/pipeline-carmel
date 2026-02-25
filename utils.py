import json
from pathlib import Path
import traceback
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import traceback
import matplotlib.pyplot as plt
import seaborn as sns
import os
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

FOLDER_DATA_PROD = Path('H:/Meu Drive') / 'data'
FOLDER_DATA_TEST = FOLDER_DATA = Path(__file__).parent / 'data'
FOLDER_DATA = FOLDER_DATA_TEST

FILE_AUTH_PROD = Path(__file__).parent / 'auth' / 'prod' / '.env'
FILE_AUTH_TEST = Path(__file__).parent / 'auth' / 'test' / '.env'
FILE_AUTH = FILE_AUTH_PROD
load_dotenv(FILE_AUTH)

FOLDER_REPORT = FOLDER_DATA / 'reports'
FOLDER_WORKS_SCHEDULED = FOLDER_DATA / 'works_scheduled'
FOLDER_SCHEDULED = FOLDER_DATA / 'scheduled'
FOLDER_WORKS_LAST_UPDATE = FOLDER_DATA / 'works_last_update'
FOLDER_FAILURE = FOLDER_DATA / 'failures'
FOLDER_ENTITY = FOLDER_DATA / 'entity'
FOLDER_FAILURE_BULK = FOLDER_DATA / 'failure_bulk'
FOLDER_WORKS_DETAILED = FOLDER_DATA / 'works_detailed'
FOLDER_FAILURE_RECORDS = FOLDER_DATA / 'failure_events'
FOLDER_WORK_RECORDS = FOLDER_DATA / 'work_events'
FOLDER_WORKS = FOLDER_DATA / 'works'
FOLDER_OPERATOR = FOLDER_DATA / 'operators'
FOLDER_COMPLEMENTARY = Path('H:/Meu Drive') / 'DASHBOARD MANUTENÇÃO' / '02 - BASES DE DADOS COMPLEMENTARES'

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        port=os.getenv('DB_PORT')
    )

def upsert_raw_data(table_name, id_column, data_list, type):
    """Realiza o Upsert no Postgres para persistência de dados brutos (Objetivo 1 e 2)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Prepara os dados para inserção (ID e o JSON completo)
    # No Infraspeak, o ID vem na raiz do objeto 'data' [8, 9]
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