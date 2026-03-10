import snowflake.connector
import json
import logging
from datetime import datetime
from config import (
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE
)
from extract import get_drug_applications
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_snowflake_connection():
    logging.info("Připojuji se ke Snowflake")
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE
    )

def setup_schema(cursor):
    logging.info("Vytvářím schema a tabulky")
    cursor.execute("CREATE SCHEMA IF NOT EXISTS RAW")
    cursor.execute("USE SCHEMA RAW")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS DRUG_APPLICATIONS (
            loaded_at TIMESTAMP,
            data VARIANT
        )
    """)

# def load_data(cursor, records):
#     logging.info(f"Nahrávám {len(records)} záznamů do DRUG_APPLICATIONS")
#     loaded_at = datetime.now().isoformat()
#     for record in records:
#         cursor.execute(
#             "INSERT INTO DRUG_APPLICATIONS (loaded_at, data) SELECT %s, PARSE_JSON(%s)",
#             (loaded_at, json.dumps(record))
#         )
#     logging.info("Nahrávání dokončeno")

def load_data(conn, records):
    logging.info(f"Připravuji {len(records)} záznamů pro nahrání")
    loaded_at = datetime.now().isoformat()
    
    df = pd.DataFrame([{
        "LOADED_AT": loaded_at,
        "DATA": json.dumps(record)
    } for record in records])
    
    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="DRUG_APPLICATIONS",
        schema="RAW",
        overwrite=False
    )
    
    logging.info(f"Nahráno {nrows} záznamů v {nchunks} chuncích")


if __name__ == "__main__":
    conn = get_snowflake_connection()
    cursor = conn.cursor()

    try:
        setup_schema(cursor)
        records = get_drug_applications()
        load_data(cursor, records)
        conn.commit()
        logging.info("Pipeline dokončena")

    except Exception as e:
        logging.error(f"Chyba: {e}")
        conn.rollback()

    finally:
        cursor.close()
        conn.close()
        logging.info("Spojení uzavřeno")