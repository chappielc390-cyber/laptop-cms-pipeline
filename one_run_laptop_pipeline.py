# one_run_laptop_pipeline.py  (Resume Mode Option 3)
# input.xlsx -> scrape (cached) -> Groq (cached) -> laptop_cms_template_<timestamp>.csv

import csv
import json
import os
import random
import re
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# =========================
# CONFIG
# =========================
BASE_DIR = Path(__file__).parent.resolve()
INPUT_XLSX = BASE_DIR / "input.xlsx"

HTML_DIR = BASE_DIR / "clean_html"
LOG_DIR = BASE_DIR / "logs"
CACHE_DIR = BASE_DIR / "groq_cache"

HTML_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
CACHE_DIR.mkdir(exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUT_CSV = BASE_DIR / f"laptop_cms_template_{TIMESTAMP}.csv"
PIPELINE_LOG = LOG_DIR / "one_run_pipeline.log"

NA = "#NA"

# If True: will detect latest existing output CSV and skip writing SKUs already present there.
# (Not required for option 3, but useful.)
RESUME_FROM_OLD_CSV = True

# =========================
# EXACT CSV HEADERS (ORDER MATTERS)
# =========================
HEADERS = [
    "sku", "base_code", "attributes__lulu_ean", "attributes__keywords", "attributes__shipping_weight",
    "attributes__brand", "attributes__product_title", "attributes__bullet_point_1", "attributes__bullet_point_2",
    "attributes__bullet_point_3", "attributes__bullet_point_4", "attributes__bullet_point_5", "attributes__bullet_point_6",
    "attributes__product_description", "attributes__lulu_product_type", "attributes__model", "attributes__weight",
    "attributes__in_the_box", "attributes__product_dimensions", "attributes__display_type", "attributes__display_resolution",
    "attributes__ram", "attributes__processor", "attributes__wifi", "attributes__bluetooth", "attributes__battery_capacity",
    "attributes__battery_type", "attributes__audio", "attributes__part_number", "attributes__web_camera", "attributes__refresh_rate",
    "attributes__storage", "attributes__version", "attributes__graphics_card", "attributes__hdmi", "attributes__usb",
    "attributes__keyboard_touchpad", "attributes__accessories", "attributes__power", "attributes__model_year",
    "attributes__no_of_channels", "attributes__country_of_origin", "attributes__color", "attributes__other_information",
    "attributes__graphic_memory", "attributes__ethernet"
]

# =========================
# GROQ CONFIG
# =========================
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise SystemExit("Missing GROQ_API_KEY. Set it (setx GROQ_API_KEY \"...\") then reopen PowerShell.")

GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL = "openai/gpt-oss-120b"

# =========================
# PLAYWRIGHT CONFIG
# =========================
HEADLESS = False
SLOW_MO = 35

MAX_SCRAPE_ATTEMPTS = 3
NAV_TIMEOUT_MS = 90000
WAIT_AFTER_GOTO_MS = 2500

# If html already exists and is >= this size, we assume it's good and skip scraping
SKIP_HTML_IF_EXISTS_OVER_BYTES = 50_000

# human-ish delay between URLs
DELAY_RANGE = (4.0, 7.0)

# =========================
# GROQ RATE LIMIT / RETRY
# =========================
MIN_DELAY_BETWEEN_CALLS = 3.5
MAX_GROQ_RETRIES = 6
BACKOFF_BASE = 2.0
BACKOFF_JITTER = (0.2, 0.8)
_last_call_ts = 0.0

# =========================
# LOGGING
# =========================
def log(line: str):
    print(line)
    PIPELINE_LOG.open("a", encoding="utf-8").write(line + "\n")

# =========================
# UTILS
# =========================
def host(url: str) -> str:
    return urlparse(url or "").netloc.lower()

def is_amazon(url: str) -> bool:
    return "amazon." in host(url)

def collapse_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip()

def pace_calls():
    global _last_call_ts
    now = time.time()
    wait = MIN_DELAY_BETWEEN_CALLS - (now - _last_call_ts)
    if wait > 0:
        time.sleep(wait)
    _last_call_ts = time.time()

def normalize_to_headers(row: dict) -> dict:
    out = {}
    for h in HEADERS:
        v = row.get(h, NA)
        if v is None or (isinstance(v, str) and v.strip() == ""):
            v = NA
        out[h] = v
    return out

def latest_output_csv() -> Path | None:
    candidates = sorted(BASE_DIR.glob("laptop_cms_template_*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
    # Exclude the one we are currently creating (may exist if rerun quickly)
    candidates = [c for c in candidates if c.name != OUT_CSV.name]
    return candidates[0] if candidates else None

def load_done_skus_from_csv(csv_path: Path) -> set[str]:
    done = set()
    try:
        with csv_path.open("r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return done
            for row in reader:
                sku = (row.get("sku") or "").strip()
                if sku:
                    done.add(sku)
    except Exception:
        pass
    return done

# =========================
# CAPTCHA/BLOCK DETECTION
# =========================
def looks_blocked_visible_text(html: str) -> bool:
    soup = BeautifulSoup(html, "lxml")
    for t in soup(["script", "style", "noscript", "svg", "canvas", "iframe"]):
        t.decompose()
    text = soup.get_text(" ", strip=True).lower()

    strong = [
        "enter the characters you see",
        "verify you are a human",
        "robot check",
        "not a robot",
        "unusual traffic",
        "automated access",
    ]
    if any(s in text for s in strong):
        return True

    if "captcha" in text and any(s in text for s in ["verify", "robot", "human", "unusual traffic"]):
        return True

    return False

# =========================
# PLAYWRIGHT: COOKIES + HUMAN ACTIONS
# =========================
def try_accept_cookies(page) -> bool:
    selectors = [
        "#sp-cc-accept",  # Amazon
        "button:has-text('Accept')",
        "button:has-text('I Accept')",
        "button:has-text('Agree')",
        "button:has-text('Accept All')",
        "button:has-text('Allow all')",
        "[data-testid='cookie-accept']",
        "[aria-label*='Accept']",
        "[id*='accept'][role='button']",
    ]
    for sel in selectors:
        try:
            page.locator(sel).first.click(timeout=2000)
            page.wait_for_timeout(800)
            return True
        except Exception:
            pass
    return False

def human_wiggle(page):
    try:
        page.mouse.move(random.randint(50, 800), random.randint(50, 600))
        page.wait_for_timeout(random.randint(200, 600))
        page.mouse.wheel(0, random.randint(500, 1200))
        page.wait_for_timeout(random.randint(400, 900))
        page.mouse.move(random.randint(50, 900), random.randint(50, 650))
    except Exception:
        pass

def wait_for_important_content(page, url: str):
    try:
        if is_amazon(url):
            page.wait_for_selector("#productTitle, #title, #centerCol", timeout=12000)
        else:
            page.wait_for_selector("h1", timeout=12000)
    except Exception:
        pass

# =========================
# HTML -> COMPACT PAYLOAD (AVOIDS 413)
# =========================
def html_to_compact_payload(html: str, max_visible_chars: int = 18000) -> dict:
    soup = BeautifulSoup(html, "lxml")
    for t in soup(["script", "style", "noscript", "svg", "canvas", "iframe"]):
        t.decompose()

    title = collapse_ws(soup.title.get_text()) if soup.title else ""
    meta_desc = ""
    md = soup.select_one("meta[name='description']")
    if md and md.get("content"):
        meta_desc = collapse_ws(md.get("content"))

    table_lines = []
    for tr in soup.select("tr"):
        cells = [collapse_ws(c.get_text(" ", strip=True)) for c in tr.find_all(["th", "td"])]
        if len(cells) >= 2:
            k, v = cells[0], cells[1]
            if k and v and len(k) <= 70 and len(v) <= 220:
                table_lines.append(f"{k}: {v}")

    seen = set()
    tables = []
    for line in table_lines:
        if line not in seen:
            tables.append(line)
            seen.add(line)
    tables_text = "\n".join(tables[:140]) if tables else NA

    body = soup.body or soup
    text = collapse_ws(body.get_text(" ", strip=True))
    if len(text) > max_visible_chars:
        head = text[: int(max_visible_chars * 0.75)]
        tail = text[-int(max_visible_chars * 0.25):]
        text = head + " ... " + tail

    return {
        "page_title": title or NA,
        "meta_description": meta_desc or NA,
        "tables_text": tables_text,
        "visible_text": text or NA,
    }

# =========================
# GROQ PROMPT + CALL
# =========================
def build_prompt(input_row: dict, payload: dict) -> str:
    return f"""
Return ONLY a valid JSON object (no markdown, no extra text).
The JSON MUST contain EXACTLY these keys (all of them):
{HEADERS}

Rules:
- If unknown, use "{NA}".
- Keep values factual and short. No promotional language.
- Bullet points must be single-line strings (no numbering).
- attributes__keywords: comma-separated search keywords.
- base_code: same as sku.
- Prefer values from tables_text when available (specs).

Mapping rules from input.xlsx (must follow):
- sku -> sku
- ean -> attributes__lulu_ean
- shipping_weight -> attributes__shipping_weight
- color -> attributes__color
- product_type -> attributes__lulu_product_type
- mm43 -> attributes__version (if not empty else "{NA}")
- category -> attributes__other_information (if not empty else "{NA}")

Input.xlsx row:
{json.dumps(input_row, ensure_ascii=False)}

Webpage content (trimmed):
{json.dumps(payload, ensure_ascii=False)}

Output the JSON object only.
""".strip()

def call_groq_with_retries(prompt: str) -> dict:
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json",
    }
    body = {
        "model": MODEL,
        "temperature": 0.2,
        "messages": [
            {"role": "system", "content": "You must output strict JSON only. No explanations."},
            {"role": "user", "content": prompt},
        ],
    }

    for attempt in range(1, MAX_GROQ_RETRIES + 1):
        pace_calls()
        r = requests.post(GROQ_URL, headers=headers, json=body, timeout=180)

        if r.status_code == 429:
            backoff = (BACKOFF_BASE ** (attempt - 1)) + random.uniform(*BACKOFF_JITTER)
            log(f"429 rate_limited attempt={attempt}/{MAX_GROQ_RETRIES} sleeping={backoff:.2f}s")
            time.sleep(backoff)
            continue

        if r.status_code == 413:
            r.raise_for_status()

        r.raise_for_status()

        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        m = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not m:
            raise ValueError(f"No JSON found. First 200 chars:\n{text[:200]}")
        return json.loads(m.group(0))

    raise RuntimeError("Max retries exceeded (rate limit / transient errors).")

# =========================
# SCRAPE (WITH CACHE)
# =========================
def read_cached_html_if_ok(sku: str) -> str | None:
    html_path = HTML_DIR / f"{sku}.html"
    if html_path.exists():
        try:
            if html_path.stat().st_size >= SKIP_HTML_IF_EXISTS_OVER_BYTES:
                return html_path.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            return None
    return None

def scrape_html(page, sku: str, url: str) -> tuple[bool, str]:
    # 1) Resume: use cached HTML if exists
    cached = read_cached_html_if_ok(sku)
    if cached:
        log(f"HTML_CACHE_HIT {sku} bytes={len(cached)}")
        return True, cached

    # 2) Otherwise scrape
    out_file = HTML_DIR / f"{sku}.html"
    shot_file = LOG_DIR / f"{sku}.png"

    log(f"FETCH {sku} {url}")
    last_err = None

    for attempt in range(1, MAX_SCRAPE_ATTEMPTS + 1):
        try:
            log(f"  attempt={attempt}/{MAX_SCRAPE_ATTEMPTS}")
            page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            page.wait_for_timeout(WAIT_AFTER_GOTO_MS)

            if try_accept_cookies(page):
                log("  cookies=accepted")

            human_wiggle(page)

            if attempt < 3:
                wait_for_important_content(page, url)
            else:
                page.wait_for_timeout(1200)

            html = page.content()

            # save
            try:
                page.screenshot(path=str(shot_file), full_page=True)
            except Exception:
                pass
            out_file.write_text(html, encoding="utf-8")

            log(f"SAVED {sku} bytes={out_file.stat().st_size} final_url={page.url} screenshot={shot_file.name}")
            return True, html

        except PWTimeout as e:
            last_err = f"TIMEOUT {e}"
            log(f"  WARN timeout: {e}")
        except Exception as e:
            last_err = f"ERROR {e}"
            log(f"  WARN error: {e}")

    # final fail: try save whatever is present
    try:
        html_fail = page.content()
        out_file.write_text(html_fail, encoding="utf-8")
    except Exception:
        html_fail = ""
    log(f"SCRAPE_FAIL {sku} | {last_err}")
    return False, html_fail

# =========================
# GROQ CACHE (OPTION 3)
# =========================
def cache_path_for_sku(sku: str) -> Path:
    return CACHE_DIR / f"{sku}.json"

def read_cached_groq_json(sku: str) -> dict | None:
    p = cache_path_for_sku(sku)
    if p.exists():
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return None
    return None

def write_cached_groq_json(sku: str, data: dict):
    p = cache_path_for_sku(sku)
    p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# =========================
# NA ROW
# =========================
def make_na_row(input_row: dict) -> dict:
    row = {h: NA for h in HEADERS}
    sku = input_row.get("sku", NA)
    row["sku"] = sku
    row["base_code"] = sku

    row["attributes__lulu_ean"] = (input_row.get("ean") or "").strip() or NA
    row["attributes__shipping_weight"] = (input_row.get("shipping_weight") or "").strip() or NA
    row["attributes__color"] = (input_row.get("color") or "").strip() or NA
    row["attributes__lulu_product_type"] = (input_row.get("product_type") or "").strip() or NA
    row["attributes__version"] = (input_row.get("mm43") or "").strip() or NA
    row["attributes__other_information"] = (input_row.get("category") or "").strip() or NA
    return row

# =========================
# MAIN
# =========================
def main():
    df = pd.read_excel(INPUT_XLSX)

    done_skus = set()
    if RESUME_FROM_OLD_CSV:
        old = latest_output_csv()
        if old:
            done_skus = load_done_skus_from_csv(old)
            log(f"RESUME_FROM_OLD_CSV found={old.name} done_skus={len(done_skus)}")
        else:
            log("RESUME_FROM_OLD_CSV no previous output found")

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADERS)
        writer.writeheader()

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=HEADLESS, slow_mo=SLOW_MO)
            context = browser.new_context(
                locale="en-US",
                timezone_id="Asia/Kolkata",
                viewport={"width": 1366, "height": 768},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                extra_http_headers={
                    "Accept-Language": "en-US,en;q=0.9",
                    "Upgrade-Insecure-Requests": "1",
                }
            )
            page = context.new_page()

            for _, r in df.iterrows():
                input_row = {
                    "sku": str(r.get("sku", "")).strip(),
                    "ean": str(r.get("ean", "")).strip(),
                    "shipping_weight": str(r.get("shipping_weight", "")).strip(),
                    "color": str(r.get("color", "")).strip(),
                    "product_type": str(r.get("product_type", "")).strip(),
                    "url": str(r.get("url", "")).strip(),
                    "mm43": str(r.get("mm43", "")).strip(),
                    "category": str(r.get("category", "")).strip(),
                }

                sku = input_row["sku"]
                url = input_row["url"]

                if not sku or not url:
                    log(f"ROW_SKIP missing sku/url sku={sku} url={url}")
                    writer.writerow(make_na_row(input_row))
                    continue

                # Optional: skip writing SKUs already in old CSV (true "resume output")
                if RESUME_FROM_OLD_CSV and sku in done_skus:
                    log(f"ROW_SKIP already_in_old_csv sku={sku}")
                    continue

                # 0) Groq cache hit? then just write row (no scrape / no groq)
                cached_groq = read_cached_groq_json(sku)
                if cached_groq:
                    final_row = normalize_to_headers(cached_groq)
                    # enforce mappings
                    final_row["sku"] = sku
                    final_row["base_code"] = sku
                    final_row["attributes__lulu_ean"] = input_row["ean"] or NA
                    final_row["attributes__shipping_weight"] = input_row["shipping_weight"] or NA
                    final_row["attributes__color"] = input_row["color"] or NA
                    final_row["attributes__lulu_product_type"] = input_row["product_type"] or NA
                    final_row["attributes__version"] = input_row["mm43"] or NA
                    if final_row["attributes__other_information"] == NA:
                        final_row["attributes__other_information"] = input_row["category"] or NA

                    writer.writerow(final_row)
                    log(f"GROQ_CACHE_HIT sku={sku} -> wrote row (no scrape/no groq)")
                    continue

                # 1) scrape html (or use HTML cache)
                ok_scrape, html = scrape_html(page, sku, url)
                if not ok_scrape or not html:
                    log(f"ROW_FAIL scrape sku={sku} -> NA row")
                    writer.writerow(make_na_row(input_row))
                    time.sleep(random.uniform(*DELAY_RANGE))
                    continue

                # 2) block check
                if looks_blocked_visible_text(html):
                    log(f"ROW_SKIP blocked_visible_text sku={sku} -> NA row")
                    writer.writerow(make_na_row(input_row))
                    time.sleep(random.uniform(*DELAY_RANGE))
                    continue

                # 3) compact payload + Groq
                try:
                    payload = html_to_compact_payload(html, max_visible_chars=18000)
                    prompt = build_prompt(input_row, payload)

                    try:
                        model_row = call_groq_with_retries(prompt)
                    except requests.HTTPError as e:
                        status = getattr(e.response, "status_code", None)
                        if status == 413:
                            log(f"413 payload_too_large sku={sku} retry_smaller")
                            payload = html_to_compact_payload(html, max_visible_chars=9000)
                            prompt = build_prompt(input_row, payload)
                            model_row = call_groq_with_retries(prompt)
                        else:
                            raise

                    # cache Groq output (OPTION 3)
                    write_cached_groq_json(sku, model_row)

                    final_row = normalize_to_headers(model_row)
                    # enforce mappings
                    final_row["sku"] = sku
                    final_row["base_code"] = sku
                    final_row["attributes__lulu_ean"] = input_row["ean"] or NA
                    final_row["attributes__shipping_weight"] = input_row["shipping_weight"] or NA
                    final_row["attributes__color"] = input_row["color"] or NA
                    final_row["attributes__lulu_product_type"] = input_row["product_type"] or NA
                    final_row["attributes__version"] = input_row["mm43"] or NA
                    if final_row["attributes__other_information"] == NA:
                        final_row["attributes__other_information"] = input_row["category"] or NA

                    writer.writerow(final_row)
                    log(f"ROW_OK sku={sku} -> wrote row + cached groq_cache/{sku}.json")

                except Exception as e:
                    log(f"ROW_FAIL groq sku={sku} err={e} -> NA row")
                    writer.writerow(make_na_row(input_row))

                time.sleep(random.uniform(*DELAY_RANGE))

            context.close()
            browser.close()

    print(f"\nDONE: {OUT_CSV}")
    print(f"LOG:  {PIPELINE_LOG}")
    print(f"HTML:  {HTML_DIR}")
    print(f"CACHE:{CACHE_DIR}")

if __name__ == "__main__":
    main()
