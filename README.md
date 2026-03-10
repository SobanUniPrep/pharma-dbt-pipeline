# Pharma Analytics Pipeline

End-to-end data pipeline pro analýzu FDA drug approvals dat.

## Architektura
```
FDA API → Python → Snowflake RAW → dbt Staging → dbt Marts
```

## Stack
- **Python** — extrakce dat z FDA API s pagination a načtení do Snowflake
- **Snowflake** — cloudový datový sklad
- **dbt** — transformace dat, testy, makra, dokumentace

## Struktura projektu
```
pharma-dbt-pipeline/
├── ingestion/
│   ├── config.py       # konfigurace a konstanty
│   ├── extract.py      # volání FDA API s pagination
│   └── load.py         # načtení do Snowflake RAW
└── dbt_pharma/
    ├── models/
    │   ├── staging/    # views, FLATTEN JSON, čištění
    │   └── marts/      # tabulky, business metriky
    ├── seeds/          # referenční číselníky
    ├── macros/         # znovupoužitelná logika
    └── tests/          # singular testy
```

## Data
**Zdroj:** FDA Open API (drug approvals)

**RAW vrstva:**
- `DRUG_APPLICATIONS` — surová data z FDA API

**Staging vrstva:**
- `stg_drug_applications` — základní info o aplikaci
- `stg_drug_products` — produkty s FLATTEN a JOIN na active ingredients
- `stg_drug_submissions` — schválení s FLATTEN

**Marts vrstva:**
- `mart_drug_approvals` — incremental model, schválené léky
- `mart_sponsor_analysis` — ranking farmaceutických firem
- `mart_product_overview` — přehled produktů s JOIN na číselník

## Spuštění

### Python pipeline
```bash
cd ingestion
pip install -r requirements.txt
python load.py
```

### dbt transformace
```bash
cd dbt_pharma
dbt seed
dbt run
dbt test
dbt docs serve
```