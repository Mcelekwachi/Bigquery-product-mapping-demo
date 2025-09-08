# dev_anon.py
from __future__ import annotations
import os, sys, json, random
from pathlib import Path
from datetime import datetime

import pandas as pd

# Optional imports (not needed in mock mode)
try:
    from pandas_gbq import read_gbq  # type: ignore
    import pydata_google_auth  # type: ignore
except Exception:  # pragma: no cover
    read_gbq = None
    pydata_google_auth = None

# ---- Config via env vars (no company names in code) ----
BQ_PROJECT_ID   = os.getenv("BQ_PROJECT_ID", "your-project-id")
BQ_LOCATION     = os.getenv("BQ_LOCATION", "EU")

# Source dataset (Plato-like)
SRC_DATASET     = os.getenv("SRC_DATASET", "source_dataset")
TBL_SALES_LINES = os.getenv("TBL_SALES_LINES", "SalesOrderSalesLines")
TBL_SALES_ORDER = os.getenv("TBL_SALES_ORDER", "SalesOrder")
TBL_SALES_ITEM  = os.getenv("TBL_SALES_ITEM", "SalesItem")

# Mart dataset (Magellan-like)
MART_DATASET    = os.getenv("MART_DATASET", "mart_sales")
TBL_MART        = os.getenv("TBL_MART", "MartEurope")

# Column names that had spaces internally
COL_MG_ORDER        = os.getenv("COL_MG_ORDER", "MG Order")
COL_MG_ORDER_LINE   = os.getenv("COL_MG_ORDER_LINE", "MG Order Line Item")
COL_MG_PRODUCT_L5   = os.getenv("COL_MG_PRODUCT_L5", "MG Product - Level 5")

# Mock mode switch
USE_MOCK = os.getenv("USE_MOCK", "0") == "1" or "--mock" in sys.argv

# ---- Helpers ----
def load_supported_entities() -> list[str]:
    here = Path(__file__).parent
    private = here / "entities.private.json"   # gitignored
    sample  = here / "entities.sample.json"    # checked in
    path = private if private.exists() else sample
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_credentials():
    if USE_MOCK:
        return None
    if pydata_google_auth is None:
        raise RuntimeError("pydata_google_auth not installed; run: pip install pydata-google-auth")
    scopes = [
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
    return pydata_google_auth.get_user_credentials(scopes)

# ---- BigQuery path (real data) ----
def fetch_company_codes(credentials) -> list[str]:
    supported = load_supported_entities()
    names_sql = f"""
    WITH supported AS (
      {" UNION ALL ".join([f"SELECT '{n}' AS name" for n in supported])}
    ),
    sup AS (
      SELECT name,
             REGEXP_REPLACE(LOWER(TRIM(name)), r'[^a-z0-9]', '') AS sup_norm
      FROM supported
    ),
    nm AS (
      SELECT DISTINCT
        CompanyCode,
        CompanyName,
        REGEXP_REPLACE(LOWER(TRIM(CompanyName)), r'[^a-z0-9]', '') AS name_norm
      FROM `{BQ_PROJECT_ID}.{MART_DATASET}.{TBL_MART}`
    )
    SELECT DISTINCT nm.CompanyCode
    FROM nm
    JOIN sup s ON nm.name_norm = s.sup_norm
    ORDER BY nm.CompanyCode
    """
    if read_gbq is None:
        raise RuntimeError("pandas-gbq not installed; run: pip install pandas-gbq")
    df = read_gbq(names_sql, project_id=BQ_PROJECT_ID, credentials=credentials, location=BQ_LOCATION)
    codes = sorted(set(df["CompanyCode"].astype(str)))
    if not codes:
        raise RuntimeError("No CompanyCodes resolved from supported entities.")
    return codes

def run_mapping_query(codes: list[str], credentials) -> pd.DataFrame:
    codes_array_sql = "ARRAY<STRING>[" + ",".join(f"'{c}'" for c in codes) + "]"
    sql = f"""
    WITH plato_keys AS (
      SELECT DISTINCT
        sol.CompanyCode AS company_code,
        sol.Dimension3  AS order_number,
        sol.LineNumber  AS order_line,
        si.SalesItemId  AS sales_item_id
      FROM `{BQ_PROJECT_ID}.{SRC_DATASET}.{TBL_SALES_LINES}` AS sol
      JOIN `{BQ_PROJECT_ID}.{SRC_DATASET}.{TBL_SALES_ORDER}` AS so
        ON so.CompanyCode = sol.CompanyCode
       AND so.SalesOrderNumber = sol.Dimension3
      JOIN `{BQ_PROJECT_ID}.{SRC_DATASET}.{TBL_SALES_ITEM}` AS si
        ON si.ObjectId = sol.SalesItem
       AND si.CompanyCode = sol.CompanyCode
      WHERE sol.CompanyCode IN UNNEST({codes_array_sql})
        AND sol.Dimension3 IS NOT NULL
        AND so.OrderEntryDate >= DATE('2025-01-01')
    ),
    mag AS (
      SELECT
        CAST(CompanyCode AS STRING)               AS company_code,
        CAST(`{COL_MG_ORDER}` AS STRING)          AS order_number,
        SAFE_CAST(`{COL_MG_ORDER_LINE}` AS INT64) AS order_line,
        `{COL_MG_PRODUCT_L5}`                     AS mg_product_name
      FROM `{BQ_PROJECT_ID}.{MART_DATASET}.{TBL_MART}`
    ),
    joined AS (
      SELECT pk.company_code, pk.sales_item_id, m.mg_product_name
      FROM plato_keys pk
      LEFT JOIN mag m USING (company_code, order_number, order_line)
      WHERE m.mg_product_name IS NOT NULL
    ),
    counts AS (
      SELECT company_code, sales_item_id, mg_product_name, COUNT(*) AS cnt
      FROM joined
      GROUP BY 1,2,3
    ),
    picked AS (
      SELECT
        company_code,
        sales_item_id,
        ARRAY_AGG(STRUCT(mg_product_name, cnt) ORDER BY cnt DESC LIMIT 1)[OFFSET(0)].mg_product_name AS mg_product_name
      FROM counts
      GROUP BY company_code, sales_item_id
    )
    SELECT company_code, sales_item_id, mg_product_name
    FROM picked
    ORDER BY company_code, sales_item_id
    """
    return read_gbq(sql, project_id=BQ_PROJECT_ID, credentials=credentials, use_bqstorage_api=True, location=BQ_LOCATION)

# ---- Mock path (synthetic data) ----
def mock_mapping_dataframe(seed: int = 7) -> pd.DataFrame:
    """
    Generate a small but realistic synthetic set:
    - ~3 companies
    - ~20 sales items used across random orders/lines
    - Each item maps to a dominant product; a few noisy lines simulate ambiguity
    """
    random.seed(seed)

    companies = ["C01", "C02", "C03"]
    items = [
        "0010-R01", "0015-R01", "0020-R01", "0030-R01", "0032-R01",
        "0040-R01", "0042-R01", "0088-R01", "1870140", "1870156",
        "3410-1036", "3440-1034", "3432-1015", "390010060"
    ]
    product_of_item = {
        "0010-R01": "010 - Venetian Blinds",
        "0015-R01": "015 - Wood Blinds",
        "0020-R01": "020 - Vertical Blinds",
        "0030-R01": "030 - Pleated Blinds",
        "0032-R01": "032 - Duette Blinds",
        "0040-R01": "040 - Roller Blinds",
        "0042-R01": "042 - Roman Shades",
        "0088-R01": "088 - Insect Screens",
        "1870140":  "040 - Roller Blinds",
        "1870156":  "098 - Undefined/Various",
        "3410-1036": "010 - Venetian Blinds",
        "3440-1034": "040 - Roller Blinds",
        "3432-1015": "032 - Duette Blinds",
        "390010060": "094 - Order Surcharges",
    }

    # Build faux order lines (plato_keys)
    rows = []
    for i in range(3000):
        cc = random.choice(companies)
        it = random.choice(items)
        order = f"25-{i:06d}"
        line = random.randint(1, 4)
        rows.append((cc, order, line, it))
    plato = pd.DataFrame(rows, columns=["company_code", "order_number", "order_line", "sales_item_id"])

    # Build mag with a little noise ( 8% of lines get a different product )
    mag_rows = []
    for r in plato.itertuples(index=False):
        base = product_of_item[r.sales_item_id]
        if random.random() < 0.08:
            # flip to a different product to simulate ambiguity
            alt = random.choice([p for p in set(product_of_item.values()) if p != base])
            prod = alt
        else:
            prod = base
        mag_rows.append((r.company_code, r.order_number, r.order_line, prod))
    mag = pd.DataFrame(mag_rows, columns=["company_code", "order_number", "order_line", "mg_product_name"])

    # Join & pick most frequent product per (company_code, sales_item_id)
    joined = (plato.merge(mag, on=["company_code", "order_number", "order_line"], how="left")
                    .dropna(subset=["mg_product_name"]))
    counts = (joined.groupby(["company_code","sales_item_id","mg_product_name"], as_index=False)
                    .size().rename(columns={"size":"cnt"}))
    picked = (counts.sort_values(["company_code","sales_item_id","cnt"], ascending=[True,True,False])
                    .groupby(["company_code","sales_item_id"], as_index=False)
                    .first()[["company_code","sales_item_id","mg_product_name"]])
    return picked.sort_values(["company_code","sales_item_id"]).reset_index(drop=True)

# ---- Main ----
def main():
    out_dir = Path.cwd() / "exports"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"product_map_{datetime.now():%Y%m%d_%H%M%S}.xlsx"

    if USE_MOCK:
        df = mock_mapping_dataframe()
    else:
        creds = get_credentials()
        codes = fetch_company_codes(creds)
        df = run_mapping_query(codes, creds)

    df.to_excel(out_path, index=False, sheet_name="mapping")  # requires openpyxl
    print(f"Saved Excel: {out_path}")
    print(df.shape)

if __name__ == "__main__":
    main()
