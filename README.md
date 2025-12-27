\# Laptop CMS Pipeline



\## What this project does

\- Reads input.xlsx

\- Scrapes laptop product pages

\- Uses Groq LLM to extract specs

\- Generates a CMS-ready CSV



\## Requirements

\- Python 3.11+

\- GROQ\_API\_KEY set as environment variable



\## Install

pip install streamlit pandas openpyxl requests beautifulsoup4 lxml playwright

python -m playwright install chromium



\## Run (CLI)

python one\_run\_laptop\_pipeline.py



\## Run (Dashboard)

streamlit run dashboard.py



\## Input file

input.xlsx with columns:

sku, ean, shipping\_weight, color, product\_type, url, mm43, category



\## Output

laptop\_cms\_template\_<timestamp>.csv



