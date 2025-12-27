import os
import sys
import time
import glob
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# BASE DIR (CLOUD + LOCAL SAFE)
# =========================
BASE_DIR = Path(__file__).parent.resolve()

PIPELINE_SCRIPT = BASE_DIR / "one_run_laptop_pipeline.py"
INPUT_XLSX = BASE_DIR / "input.xlsx"
HTML_DIR = BASE_DIR / "clean_html"
LOG_DIR = BASE_DIR / "logs"
CACHE_DIR = BASE_DIR / "groq_cache"

PIPELINE_LOG = LOG_DIR / "one_run_pipeline.log"

HTML_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

st.set_page_config(page_title="Laptop CMS Pipeline", layout="wide")

# =========================
# PLAYWRIGHT CLOUD FIX
# =========================
def ensure_playwright_browser():
    """
    Streamlit Cloud does NOT install Chromium automatically.
    This installs it once and marks completion.
    """
    marker = BASE_DIR / ".pw_installed"
    if marker.exists():
        return

    with st.spinner("Installing Playwright Chromium (first run only)…"):
        try:
            subprocess.run(
                [sys.executable, "-m", "playwright", "install", "chromium"],
                check=True,
                capture_output=True,
                text=True,
            )
            marker.write_text("ok")
        except Exception as e:
            st.error("Playwright browser install failed.")
            st.code(str(e))
            st.stop()

ensure_playwright_browser()

# =========================
# HELPERS
# =========================
def latest_output_csv():
    files = sorted(
        BASE_DIR.glob("laptop_cms_template_*.csv"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None

def tail_text(path: Path, max_chars=12000):
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="ignore")[-max_chars:]

def open_folder(folder: Path):
    if os.name == "nt" and folder.exists():
        subprocess.Popen(["explorer", str(folder)])

def sku_status_table(df: pd.DataFrame):
    rows = []
    for _, r in df.iterrows():
        sku = str(r.get("sku", "")).strip()
        rows.append({
            "sku": sku,
            "html": (HTML_DIR / f"{sku}.html").exists(),
            "screenshot": (LOG_DIR / f"{sku}.png").exists(),
            "groq_cache": (CACHE_DIR / f"{sku}.json").exists(),
        })
    return pd.DataFrame(rows)

def run_pipeline():
    if not os.getenv("GROQ_API_KEY"):
        st.error("GROQ_API_KEY not set in Streamlit Secrets.")
        return

    proc = subprocess.Popen(
        ["python", str(PIPELINE_SCRIPT)],
        cwd=str(BASE_DIR),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    live = st.empty()

    output = ""
    while True:
        line = proc.stdout.readline()
        if line:
            output += line
            output = output[-12000:]
            live.code(output)
        if proc.poll() is not None:
            break
        time.sleep(0.2)

    st.success("Pipeline finished")

# =========================
# UI
# =========================
st.title("Laptop CMS Pipeline Dashboard")

st.subheader("Input file")
upload = st.file_uploader("Upload input.xlsx", type=["xlsx"])
if upload:
    INPUT_XLSX.write_bytes(upload.getvalue())
    st.success("input.xlsx uploaded")

if INPUT_XLSX.exists():
    df = pd.read_excel(INPUT_XLSX)
    st.write(f"Rows in input.xlsx: {len(df)}")
else:
    st.warning("input.xlsx not found")

st.divider()

if st.button("▶ Run pipeline"):
    run_pipeline()

st.divider()

st.subheader("Live log")
st.code(tail_text(PIPELINE_LOG))

st.divider()

st.subheader("SKU status")
if INPUT_XLSX.exists():
    st.dataframe(sku_status_table(pd.read_excel(INPUT_XLSX)), use_container_width=True)

st.divider()

latest = latest_output_csv()
if latest:
    st.download_button(
        "Download latest CSV",
        latest.read_bytes(),
        file_name=latest.name,
        mime="text/csv",
    )
