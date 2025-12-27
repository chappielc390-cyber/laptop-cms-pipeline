import os
import time
import glob
import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# CONFIG (match your project)
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
# HELPERS
# =========================
def latest_output_csv():
    files = sorted(BASE_DIR.glob("laptop_cms_template_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None

def tail_text(path: Path, max_chars=12000) -> str:
    if not path.exists():
        return ""
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
        return txt[-max_chars:]
    except Exception:
        return ""

def open_folder(folder: Path):
    # Windows Explorer open
    if folder.exists():
        subprocess.Popen(["explorer", str(folder)])

def read_input_df() -> pd.DataFrame:
    if INPUT_XLSX.exists():
        return pd.read_excel(INPUT_XLSX)
    return pd.DataFrame()

def sku_status_table(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in df.iterrows():
        sku = str(r.get("sku", "")).strip()
        url = str(r.get("url", "")).strip()

        html_path = HTML_DIR / f"{sku}.html"
        png_path = LOG_DIR / f"{sku}.png"
        cache_path = CACHE_DIR / f"{sku}.json"

        rows.append({
            "sku": sku,
            "url": url[:80] + ("..." if len(url) > 80 else ""),
            "html_saved": html_path.exists(),
            "html_size_kb": round(html_path.stat().st_size / 1024, 1) if html_path.exists() else 0,
            "screenshot_saved": png_path.exists(),
            "groq_cached": cache_path.exists(),
        })
    return pd.DataFrame(rows)

def run_pipeline_blocking():
    """
    Runs pipeline in a subprocess and streams log while running.
    """
    if not PIPELINE_SCRIPT.exists():
        st.error(f"Pipeline script not found: {PIPELINE_SCRIPT}")
        return

    # Check key presence
    if not os.getenv("GROQ_API_KEY"):
        st.error("GROQ_API_KEY is not set in environment. Set it using setx and reopen terminal.")
        return

    # Start pipeline process
    cmd = ["python", str(PIPELINE_SCRIPT)]
    proc = subprocess.Popen(cmd, cwd=str(BASE_DIR), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

    live_out = st.empty()
    live_log = st.empty()

    captured = ""

    while True:
        line = proc.stdout.readline() if proc.stdout else ""
        if line:
            captured += line
            # keep last part of stdout
            captured = captured[-12000:]
            live_out.code(captured)

        # also show pipeline log tail (more reliable)
        live_log.code(tail_text(PIPELINE_LOG))

        if proc.poll() is not None:
            # process finished
            break

        time.sleep(0.2)

    # Final update
    live_out.code(captured)
    live_log.code(tail_text(PIPELINE_LOG))
    st.success("Pipeline finished.")


# =========================
# UI
# =========================
st.title("Laptop CMS Pipeline Dashboard")

colA, colB, colC = st.columns([1.2, 1, 1])

with colA:
    st.subheader("Input file")
    st.write(f"Using: `{INPUT_XLSX}`")

    upload = st.file_uploader("Upload new input.xlsx (optional)", type=["xlsx"])
    if upload is not None:
        # Save uploaded file as input.xlsx
        INPUT_XLSX.write_bytes(upload.getvalue())
        st.success("Saved uploaded file to input.xlsx")

with colB:
    st.subheader("Quick actions")
    if st.button("Open project folder"):
        open_folder(BASE_DIR)
    if st.button("Open logs folder"):
        open_folder(LOG_DIR)
    if st.button("Open clean_html folder"):
        open_folder(HTML_DIR)
    if st.button("Open groq_cache folder"):
        open_folder(CACHE_DIR)

with colC:
    st.subheader("Latest output")
    latest = latest_output_csv()
    if latest:
        st.write(f"Latest CSV: `{latest.name}`")
        st.download_button(
            "Download latest CSV",
            data=latest.read_bytes(),
            file_name=latest.name,
            mime="text/csv"
        )
    else:
        st.write("No output CSV found yet.")

st.divider()

st.subheader("Run")
run_col1, run_col2 = st.columns([1, 2])

with run_col1:
    st.write("Make sure you have:")
    st.write("- GROQ_API_KEY set")
    st.write("- Playwright installed")
    st.write("- input.xlsx ready")
    if st.button("▶ Run pipeline now"):
        run_pipeline_blocking()

with run_col2:
    st.write("Live pipeline log (tail):")
    st.code(tail_text(PIPELINE_LOG), language="text")
    if st.button("Refresh log"):
        st.rerun()

st.divider()

st.subheader("SKU status")
df = read_input_df()
if df.empty:
    st.warning("input.xlsx not found or empty.")
else:
    st.write(f"Rows in input.xlsx: {len(df)}")
    status_df = sku_status_table(df)
    st.dataframe(status_df, use_container_width=True)

    st.caption("Tip: If groq_cached is True, resume mode will skip Groq calls for that SKU.")
