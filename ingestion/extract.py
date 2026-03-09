import requests
import logging
from config import FDA_BASE_URL, BATCH_SIZE, TOTAL_RECORDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def get_drug_applications():
    all_records = []
    skip = 0
    
    while skip < TOTAL_RECORDS:
        logging.info(f"Stahuji záznamy {skip} - {skip + BATCH_SIZE}")
        response = requests.get(
            FDA_BASE_URL,
            params={
                "limit": BATCH_SIZE,
                "skip": skip
            }
        )
        response.raise_for_status()
        data = response.json()
        batch = data["results"]
        all_records.extend(batch)
        skip += BATCH_SIZE
    
    logging.info(f"Celkem staženo: {len(all_records)} záznamů")
    return all_records

if __name__ == "__main__":
    records = get_drug_applications()
    logging.info("Extrakce dokončena")