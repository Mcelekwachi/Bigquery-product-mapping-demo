# Bigquery-product-mapping-demo
A reproducible BigQuery/Pandas workflow that maps order-line items to product families. Includes a mock mode for running without cloud access and an anonymized configuration for safe open-source sharing.

Run the project with synthetic data so reviewers can try it without cloud access.

```bash
python -m venv .venv && . .venv/Scripts/activate    # or: source .venv/bin/activate
pip install pandas openpyxl

# run with mock data
set USE_MOCK=1        # PowerShell: $env:USE_MOCK="1" ; macOS/Linux: export USE_MOCK=1
python dev_anon.py    # or: python dev_anon.py --mock
