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

def load_data(cursor, records):
    logging.info(f"Nahrávám {len(records)} záznamů do DRUG_APPLICATIONS")
    loaded_at = datetime.now().isoformat()
    for record in records:
        cursor.execute(
            "INSERT INTO DRUG_APPLICATIONS (loaded_at, data) SELECT %s, PARSE_JSON(%s)",
            (loaded_at, json.dumps(record))
        )
    logging.info("Nahrávání dokončeno")

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