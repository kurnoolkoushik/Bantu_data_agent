
import re
import sqlite3
import textwrap
from datetime import date, timedelta
from pathlib import Path
from random import choice, randint, uniform

import pandas as pd
import plotly.express as px
import streamlit as st

# ─────────────────────────────────────────────
# 0. CONFIG / CONSTANTS
# ─────────────────────────────────────────────
DB_PATH   = "askdata.db"           # SQLite file created next to this script
TABLE     = "sales_data"           # primary demo table
REGIONS   = ["North", "South", "East", "West", "Central"]
CUSTOMERS = [
    "Acme Corp", "Globex Inc", "Initech", "Umbrella Ltd",
    "Stark Industries", "Wayne Enterprises", "Hooli", "Pied Piper",
    "Dunder Mifflin", "Vandelay Industries",
]

# ─────────────────────────────────────────────
# 1. DATABASE BOOTSTRAP
# ─────────────────────────────────────────────
def init_db() -> None:
    """Create the SQLite database + seed table if they don't exist yet."""
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            invoice_id    TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            amount        REAL NOT NULL,
            invoice_date  TEXT NOT NULL,   -- stored as YYYY-MM-DD
            region        TEXT NOT NULL
        )
    """)
    con.commit()

    # Only seed when the table is empty so we don't duplicate on restarts
    if cur.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0] == 0:
        today = date.today()
        rows  = []
        for i in range(1, 201):                   # 200 seed rows
            d = today - timedelta(days=randint(0, 364))
            rows.append((
                f"INV-{i:04d}",
                choice(CUSTOMERS),
                round(uniform(500, 50_000), 2),
                d.isoformat(),
                choice(REGIONS),
            ))
        cur.executemany(
            f"INSERT INTO {TABLE} VALUES (?,?,?,?,?)", rows
        )
        con.commit()

    con.close()


def get_schema() -> str:
    """Return the CREATE TABLE schema string for the demo table."""
    con = sqlite3.connect(DB_PATH)
    schema = con.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (TABLE,)
    ).fetchone()[0]
    con.close()
    return schema


def get_columns() -> list[str]:
    """Return column names for the primary demo table."""
    con  = sqlite3.connect(DB_PATH)
    cols = [row[1] for row in con.execute(f"PRAGMA table_info({TABLE})")]
    con.close()
    return cols

def get_db_explorer() -> dict:
    """
    Scan all SQLite .db files in the current directory.
    Returns a dict: { db_filename: [(table_name, row_count), ...] }
    """
    result = {}
    for db_file in sorted(Path(".").glob("*.db")):
        try:
            con = sqlite3.connect(str(db_file))
            tables = [
                row[0] for row in
                con.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            ]
            table_info = []
            for t in tables:
                try:
                    cnt = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                except Exception:
                    cnt = "?"
                table_info.append((t, cnt))
            con.close()
            result[db_file.name] = table_info
        except Exception:
            pass
    return result



def sql_generator(question: str) -> str:
    """
    Convert a natural-language question into a SQL query.

    Strategy (hackathon-grade):
      • Keyword rules cover the most common analytical queries.
      • Falls back to a full-table SELECT so the app always returns *something*.
      • In production you would swap this with an LLM (e.g. GPT-4 / Gemini).
    """
    q = question.lower().strip()

    # ── Aggregate helpers ──────────────────────────────────────────
    if re.search(r"\btotal.*(sales|revenue|amount)\b", q):
        if re.search(r"\bby region\b", q):
            return f"SELECT region, ROUND(SUM(amount),2) AS total_amount FROM {TABLE} GROUP BY region ORDER BY total_amount DESC"
        if re.search(r"\bby customer\b", q):
            return f"SELECT customer_name, ROUND(SUM(amount),2) AS total_amount FROM {TABLE} GROUP BY customer_name ORDER BY total_amount DESC"
        if re.search(r"\bby (month|monthly)\b", q):
            return (f"SELECT SUBSTR(invoice_date,1,7) AS month, "
                    f"ROUND(SUM(amount),2) AS total_amount "
                    f"FROM {TABLE} GROUP BY month ORDER BY month")
        return f"SELECT ROUND(SUM(amount),2) AS total_sales FROM {TABLE}"

    if re.search(r"\baverage.*(amount|sale|order)\b", q):
        if re.search(r"\bby region\b", q):
            return f"SELECT region, ROUND(AVG(amount),2) AS avg_amount FROM {TABLE} GROUP BY region ORDER BY avg_amount DESC"
        return f"SELECT ROUND(AVG(amount),2) AS average_amount FROM {TABLE}"

    if re.search(r"\bcount\b.*\b(invoice|order|record|row|transaction)\b", q) or \
       re.search(r"\bhow many\b", q):
        if re.search(r"\bby region\b", q):
            return f"SELECT region, COUNT(*) AS invoice_count FROM {TABLE} GROUP BY region ORDER BY invoice_count DESC"
        if re.search(r"\bby customer\b", q):
            return f"SELECT customer_name, COUNT(*) AS invoice_count FROM {TABLE} GROUP BY customer_name ORDER BY invoice_count DESC"
        return f"SELECT COUNT(*) AS total_invoices FROM {TABLE}"

    # ── Ranking helpers ────────────────────────────────────────────
    if re.search(r"\btop\s*(\d+)\b", q):
        n = re.search(r"\btop\s*(\d+)\b", q).group(1)
        if re.search(r"\bcustomer\b", q):
            return (f"SELECT customer_name, ROUND(SUM(amount),2) AS total_amount "
                    f"FROM {TABLE} GROUP BY customer_name "
                    f"ORDER BY total_amount DESC LIMIT {n}")
        if re.search(r"\bregion\b", q):
            return (f"SELECT region, ROUND(SUM(amount),2) AS total_amount "
                    f"FROM {TABLE} GROUP BY region "
                    f"ORDER BY total_amount DESC LIMIT {n}")
        return f"SELECT * FROM {TABLE} ORDER BY amount DESC LIMIT {n}"

    if re.search(r"\bhighest\b|\blargest\b|\bbiggest\b", q):
        return f"SELECT * FROM {TABLE} ORDER BY amount DESC LIMIT 10"

    if re.search(r"\blowest\b|\bsmallest\b", q):
        return f"SELECT * FROM {TABLE} ORDER BY amount ASC LIMIT 10"

    # ── Time-range helpers ─────────────────────────────────────────
    if re.search(r"\blast\s*30\s*days?\b", q):
        return (f"SELECT * FROM {TABLE} "
                f"WHERE invoice_date >= DATE('now','-30 days') "
                f"ORDER BY invoice_date DESC")

    if re.search(r"\blast\s*(\d+)\s*days?\b", q):
        n = re.search(r"\blast\s*(\d+)\s*days?\b", q).group(1)
        return (f"SELECT * FROM {TABLE} "
                f"WHERE invoice_date >= DATE('now','-{n} days') "
                f"ORDER BY invoice_date DESC")

    if re.search(r"\bthis month\b|\bcurrent month\b", q):
        return (f"SELECT * FROM {TABLE} "
                f"WHERE SUBSTR(invoice_date,1,7) = SUBSTR(DATE('now'),1,7) "
                f"ORDER BY invoice_date DESC")

    # ── Filter by region ───────────────────────────────────────────
    for region in REGIONS:
        if region.lower() in q:
            return (f"SELECT * FROM {TABLE} "
                    f"WHERE region = '{region}' "
                    f"ORDER BY invoice_date DESC")

    # ── Monthly trend / time-series ────────────────────────────────
    if re.search(r"\bmonthly\b|\bover time\b|\btrend\b|\bby month\b", q):
        return (f"SELECT SUBSTR(invoice_date,1,7) AS month, "
                f"ROUND(SUM(amount),2) AS total_amount "
                f"FROM {TABLE} GROUP BY month ORDER BY month")

    # ── Specific column selection ──────────────────────────────────
    if re.search(r"\bshow\b.*\ball\b|\blist\b.*\brecords?\b|\bshow\b.*\bdata\b|\ball\s+data\b", q):
        return f"SELECT * FROM {TABLE} LIMIT 50"

    # ── Fallback: return everything (capped) ───────────────────────
    return f"SELECT * FROM {TABLE} LIMIT 50"


# ─────────────────────────────────────────────
# 3. QUERY EXECUTOR
# ─────────────────────────────────────────────
def run_query(sql: str) -> pd.DataFrame:
    """
    Execute a SQL string against the SQLite database.
    Returns a DataFrame on success, raises on error.
    """
    con = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, con)
    finally:
        con.close()
    return df


# ─────────────────────────────────────────────
# 4. CHART DETECTOR
# ─────────────────────────────────────────────
def detect_chart(df: pd.DataFrame):
    """
    Auto-select and render the best Plotly chart for the result set.

    Rules
    ─────
    • date col  + 1 numeric col  →  Line chart (time-series trend)
    • 1 text col + 1 numeric col →  Bar chart  (categorical comparison)
    • Otherwise                  →  No chart   (raw table is enough)
    """
    if df is None or df.empty or len(df.columns) < 2:
        return None

    cols         = df.columns.tolist()
    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    text_cols    = [c for c in cols if c not in numeric_cols]

    # Detect a date-like column by name or dtype
    date_cols = [
        c for c in text_cols
        if re.search(r"\bdate\b|\bmonth\b|\bday\b|\btime\b|\bperiod\b", c, re.I)
    ]

    # ── Time-series line chart ─────────────────────────────────────
    if date_cols and numeric_cols:
        fig = px.line(
            df, x=date_cols[0], y=numeric_cols[0],
            markers=True,
            title=f"{numeric_cols[0].replace('_',' ').title()} over {date_cols[0].replace('_',' ').title()}",
        )
        fig.update_layout(paper_bgcolor="white", plot_bgcolor="#f8f8ff", font_color="#1e1b4b")
        return fig

    # ── Categorical bar chart ──────────────────────────────────────
    if text_cols and numeric_cols and len(df) > 1:
        fig = px.bar(
            df, x=text_cols[0], y=numeric_cols[0],
            title=f"{numeric_cols[0].replace('_',' ').title()} by {text_cols[0].replace('_',' ').title()}",
            color=numeric_cols[0],
            color_continuous_scale="Purples",
        )
        fig.update_layout(paper_bgcolor="white", plot_bgcolor="#f8f8ff", font_color="#1e1b4b")
        return fig

    return None   # no suitable chart found


# ─────────────────────────────────────────────
# 5. FILE UPLOAD → DB APPEND / AUTO-CREATE
# ─────────────────────────────────────────────
def _pandas_dtype_to_sqlite(dtype) -> str:
    """Map a pandas dtype to an appropriate SQLite affinity."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "REAL"
    return "TEXT"


def _sanitize_table_name(name: str) -> str:
    """Turn a filename/string into a safe SQLite table name."""
    import re as _re
    name = _re.sub(r"[^\w]", "_", name.lower())
    name = _re.sub(r"_+", "_", name).strip("_")
    return name or "uploaded_table"


def _df_to_rows(df: "pd.DataFrame") -> list:
    """
    Convert a DataFrame to a list of tuples with native Python types
    that sqlite3 can handle — maps numpy int/float → int/float,
    NaN/NaT → None, everything else → str if not already scalar.
    """
    import math
    result = []
    for row in df.itertuples(index=False, name=None):
        clean = []
        for val in row:
            if val is None:
                clean.append(None)
            elif isinstance(val, float) and math.isnan(val):
                clean.append(None)
            elif hasattr(val, "item"):          # numpy scalar → Python scalar
                clean.append(val.item())
            elif hasattr(val, "isoformat"):     # date/datetime → ISO string
                clean.append(val.isoformat())
            else:
                clean.append(val)
        result.append(tuple(clean))
    return result


def append_uploaded_file(uploaded_file) -> tuple[bool, str]:
    """
    Smart upload handler:
      1. Read the Excel file.
      2. Compare its columns against the primary sales_data table.
      3a. If schema matches (≥50% column overlap) → append to sales_data.
      3b. Different schema                        → auto-create new table
          named after the file with inferred types.

    Returns (success: bool, message: str)
    """
    try:
        ext = getattr(uploaded_file, "name", "").lower()
        if ext.endswith(".csv"):
            df_upload = pd.read_csv(uploaded_file)
        elif ext.endswith((".pdf")):
            import PyPDF2
            reader = PyPDF2.PdfReader(uploaded_file)
            text = chr(10).join(page.extract_text() or "" for page in reader.pages)
            df_upload = pd.DataFrame({"extracted_text": [text.strip()]})
        elif ext.endswith((".docx", ".doc")):
            import docx
            doc = docx.Document(uploaded_file)
            text = chr(10).join(p.text for p in doc.paragraphs if p.text.strip())
            df_upload = pd.DataFrame({"extracted_text": [text.strip()]})
        else:
            # Fallback / Excel
            df_upload = pd.read_excel(uploaded_file)
    except Exception as exc:
        return False, f"Could not read file '{getattr(uploaded_file, 'name', '')}': {exc}"

    # Normalise column names
    df_upload.columns = [c.strip() for c in df_upload.columns]
    if df_upload.empty:
        return False, "Excel file is empty."

    existing_cols = set(get_columns())
    upload_cols   = set(df_upload.columns.tolist())
    matched       = existing_cols & upload_cols
    overlap_pct   = len(matched) / max(len(existing_cols), 1)

    con = sqlite3.connect(DB_PATH)
    try:
        # ── PATH A: schema matches → append to existing table ──────
        if overlap_pct >= 0.5:
            df_filtered = df_upload[[c for c in df_upload.columns if c in existing_cols]].copy()

            if "invoice_date" in df_filtered.columns:
                df_filtered["invoice_date"] = pd.to_datetime(
                    df_filtered["invoice_date"], errors="coerce"
                ).dt.strftime("%Y-%m-%d")
            if "invoice_id" in df_filtered.columns:
                df_filtered.dropna(subset=["invoice_id"], inplace=True)
            if "amount" in df_filtered.columns:
                df_filtered.dropna(subset=["amount"], inplace=True)

            if df_filtered.empty:
                return False, "No valid rows after cleaning."

            cols_str     = ", ".join(df_filtered.columns)
            placeholders = ", ".join(["?"] * len(df_filtered.columns))
            con.executemany(
                f"INSERT OR IGNORE INTO {TABLE} ({cols_str}) VALUES ({placeholders})",
                _df_to_rows(df_filtered)
            )
            con.commit()
            total = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
            return True, (
                f"✅ **Appended** {len(df_filtered):,} row(s) to `{TABLE}` "
                f"(matched columns: `{'`, `'.join(sorted(matched))}`).\n"
                f"Table now has **{total:,}** records."
            )

        # ── PATH B: different schema → auto-create new table ───────
        raw_name  = getattr(uploaded_file, "name", "uploaded_table")
        raw_name  = raw_name.rsplit(".", 1)[0]
        new_table = _sanitize_table_name(raw_name)

        existing_tables = {
            row[0] for row in
            con.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        base, suffix = new_table, 2
        while new_table in existing_tables:
            new_table = f"{base}_{suffix}"
            suffix += 1

        # Infer column types from pandas dtypes
        col_defs = ", ".join(
            f'"{col}" {_pandas_dtype_to_sqlite(df_upload[col].dtype)}'
            for col in df_upload.columns
        )
        con.execute(f'CREATE TABLE "{new_table}" ({col_defs})')

        # Convert datetime cols → ISO strings before row extraction
        df_clean = df_upload.copy()
        for col in df_clean.select_dtypes(include=["datetime64", "datetimetz"]).columns:
            df_clean[col] = df_clean[col].dt.strftime("%Y-%m-%d")
        df_clean = df_clean.where(pd.notnull(df_clean), None)

        placeholders = ", ".join(["?"] * len(df_clean.columns))
        cols_str     = ", ".join(f'"{c}"' for c in df_clean.columns)
        con.executemany(
            f'INSERT INTO "{new_table}" ({cols_str}) VALUES ({placeholders})',
            df_clean.values.tolist()
        )
        con.commit()
        total = con.execute(f'SELECT COUNT(*) FROM "{new_table}"').fetchone()[0]
        return True, (
            f"🆕 **New table created:** `{new_table}`\n"
            f"Schema auto-detected from file — "
            f"{len(df_clean.columns)} column(s): `{'`, `'.join(df_clean.columns)}`.\n"
            f"Inserted **{total:,}** row(s).\n\n"
            f"The new table is now accessible in the sidebar and queryable via the Query tab."
        )

    except Exception as exc:
        return False, f"Database error: {exc}"
    finally:
        con.close()



# ─────────────────────────────────────────────
# 6. STREAMLIT UI
# ─────────────────────────────────────────────
def main():
    # ── Page config ───────────────────────────────────────────────
    st.set_page_config(
        page_title="Bantu — The Data Agent",
        page_icon="🤖",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # ── Custom CSS (dark glassmorphism) ───────────────────────────
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');

    /* ── Light base background ──────────────────────────────────── */
    .stApp {
        background: #f5f4ff;
        font-family: 'Inter', sans-serif;
    }
    
    /* ── Hide top white header background but keep buttons ── */
    [data-testid="stHeader"] {
        background: transparent !important;
    }
    
    /* Make the top-right buttons dark so they are visible on light background */
    [data-testid="stHeader"] button * {
        color: #1e1b4b !important;
        fill: #1e1b4b !important;
    }

    .stApp::before {
        content: '';
        position: fixed;
        inset: 0;
        background:
            radial-gradient(ellipse 80% 60% at 20% 10%, rgba(108,99,255,0.10) 0%, transparent 60%),
            radial-gradient(ellipse 60% 50% at 80% 90%, rgba(72,202,228,0.08) 0%, transparent 60%),
            radial-gradient(ellipse 50% 40% at 60% 40%, rgba(168,85,247,0.06) 0%, transparent 60%);
        pointer-events: none;
        z-index: 0;
        animation: bgPulse 12s ease-in-out infinite alternate;
    }
    @keyframes bgPulse {
        0%   { opacity: 0.7; }
        100% { opacity: 1.0; }
    }

    /* ── Floating orbs ──────────────────────────────────────────── */
    .orb {
        position: fixed;
        border-radius: 50%;
        filter: blur(80px);
        pointer-events: none;
        z-index: 0;
        animation: orbFloat 20s ease-in-out infinite;
    }
    .orb-1 { width:420px; height:420px; background:rgba(108,99,255,0.07); top:-100px; left:-100px; }
    .orb-2 { width:350px; height:350px; background:rgba(72,202,228,0.05); bottom:0; right:-80px; animation-delay:-7s; }
    .orb-3 { width:260px; height:260px; background:rgba(168,85,247,0.06); top:45%; left:50%; animation-delay:-13s; }
    @keyframes orbFloat {
        0%,100% { transform: translate(0,0) scale(1); }
        33%      { transform: translate(30px,-40px) scale(1.05); }
        66%      { transform: translate(-20px,30px) scale(0.96); }
    }

    /* ── Hero section ───────────────────────────────────────────── */
    .hero-wrap {
        text-align: center;
        padding: 2.5rem 1rem 1.5rem;
        position: relative;
    }
    .hero-eyebrow {
        display: inline-block;
        background: rgba(108,99,255,0.15);
        border: 1px solid rgba(108,99,255,0.45);
        color: #9D96FF;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        padding: 5px 18px;
        border-radius: 30px;
        margin-bottom: 18px;
    }
    .hero-title {
        font-family: 'Inter', sans-serif;
        font-size: 3.4rem;
        font-weight: 900;
        line-height: 1.1;
        background: linear-gradient(120deg, #a78bfa 0%, #6C63FF 35%, #48CAE4 70%, #34d399 100%);
        background-size: 200% auto;
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        animation: shimmer 4s linear infinite;
        margin: 0 0 10px;
        letter-spacing: -0.02em;
    }
    @keyframes shimmer {
        0%   { background-position: 0% center; }
        100% { background-position: 200% center; }
    }
    .hero-sub {
        color: #5a6272;
        font-size: 1.1rem;
        font-weight: 400;
        margin: 0 auto 28px;
        max-width: 520px;
        line-height: 1.6;
        text-align: center;
        padding: 0 1rem;
    }

    /* ── Metric pill row ────────────────────────────────────────── */
    .metric-row {
        display: flex;
        gap: 14px;
        justify-content: center;
        flex-wrap: wrap;
        margin-bottom: 32px;
    }
    .metric-pill {
        background: rgba(255,255,255,0.75);
        border: 1px solid rgba(108,99,255,0.18);
        border-radius: 50px;
        padding: 10px 22px;
        display: flex;
        align-items: center;
        gap: 10px;
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        box-shadow: 0 2px 8px rgba(108,99,255,0.08);
        transition: border-color .3s, box-shadow .3s;
    }
    .metric-pill:hover {
        border-color: rgba(108,99,255,0.5);
        box-shadow: 0 0 20px rgba(108,99,255,0.15);
    }
    .metric-icon { font-size: 1.2rem; }
    .metric-val  { font-size: 1.1rem; font-weight: 700; color: #1e1b4b; }
    .metric-lab  { font-size: 0.72rem; color: #6b6f7e; font-weight: 500; text-transform: uppercase; letter-spacing: 0.08em; }

    /* ── Premium glass card ─────────────────────────────────────── */
    .glass-card {
        background: rgba(255,255,255,0.72);
        border: 1px solid rgba(108,99,255,0.14);
        border-radius: 20px;
        padding: 28px 32px;
        backdrop-filter: blur(20px);
        -webkit-backdrop-filter: blur(20px);
        box-shadow:
            0 4px 24px rgba(108,99,255,0.07),
            0 1px 0 rgba(255,255,255,0.9) inset;
        margin-bottom: 20px;
        position: relative;
        overflow: hidden;
        transition: box-shadow 0.3s, border-color 0.3s;
    }
    .glass-card::before {
        content: '';
        position: absolute;
        top: 0; left: 0; right: 0;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(108,99,255,0.4), rgba(72,202,228,0.3), transparent);
    }
    .glass-card:hover {
        border-color: rgba(108,99,255,0.25);
        box-shadow:
            0 8px 32px rgba(108,99,255,0.12),
            0 1px 0 rgba(255,255,255,0.9) inset;
    }

    /* ── Section headers inside cards ───────────────────────────── */
    .card-header {
        display: flex;
        align-items: center;
        gap: 10px;
        font-size: 1.05rem;
        font-weight: 700;
        color: #1e1b4b;
        margin-bottom: 18px;
    }
    .card-header-icon {
        width: 36px; height: 36px;
        border-radius: 10px;
        display: flex; align-items: center; justify-content: center;
        font-size: 1rem;
        background: rgba(108,99,255,0.2);
        border: 1px solid rgba(108,99,255,0.3);
    }

    /* ── SQL chip ───────────────────────────────────────────────── */
    .sql-chip {
        background: #f0f0ff;
        border: 1px solid rgba(108,99,255,0.2);
        border-left: 3px solid #6C63FF;
        border-radius: 12px;
        padding: 16px 20px;
        font-family: 'JetBrains Mono', 'Fira Code', 'Courier New', monospace;
        font-size: 0.87rem;
        color: #3730a3;
        white-space: pre-wrap;
        line-height: 1.7;
        box-shadow: 0 2px 8px rgba(108,99,255,0.08);
        position: relative;
        overflow: hidden;
    }
    .sql-chip::before {
        content: 'SQL';
        position: absolute;
        top: 10px; right: 14px;
        font-size: 0.62rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        color: rgba(108,99,255,0.7);
        font-family: 'Inter', sans-serif;
    }

    /* ── Sidebar ────────────────────────────────────────────────── */
    section[data-testid="stSidebar"] {
        background: rgba(245,244,255,0.97) !important;
        border-right: 1px solid rgba(108,99,255,0.12) !important;
        backdrop-filter: blur(20px);
    }
    section[data-testid="stSidebar"]::before {
        content: '';
        position: absolute;
        top: 0; bottom: 0; right: 0;
        width: 1px;
        background: linear-gradient(180deg, transparent, rgba(108,99,255,0.5) 30%, rgba(72,202,228,0.3) 70%, transparent);
    }
    .sidebar-logo {
        display: flex;
        align-items: center;
        gap: 10px;
        padding: 8px 0 16px;
    }
    .sidebar-logo-icon {
        width: 40px; height: 40px;
        border-radius: 12px;
        background: linear-gradient(135deg, #6C63FF, #48CAE4);
        display: flex; align-items: center; justify-content: center;
        font-size: 1.2rem;
        box-shadow: 0 4px 15px rgba(108,99,255,0.4);
    }
    .sidebar-logo-text {
        font-size: 1.1rem;
        font-weight: 800;
        background: linear-gradient(90deg, #9D96FF, #48CAE4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
    }
    .sidebar-section {
        background: rgba(255,255,255,0.03);
        border: 1px solid rgba(255,255,255,0.07);
        border-radius: 14px;
        padding: 14px 16px;
        margin-bottom: 14px;
    }
    .sidebar-section-title {
        font-size: 0.68rem;
        font-weight: 700;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: rgba(108,99,255,0.9);
        margin-bottom: 10px;
    }
    .sample-q {
        display: block;
        padding: 7px 12px;
        margin: 4px 0;
        background: rgba(108,99,255,0.05);
        border: 1px solid rgba(108,99,255,0.15);
        border-radius: 8px;
        color: #4338ca;
        font-size: 0.82rem;
        cursor: pointer;
        transition: all 0.2s;
    }
    .sample-q:hover {
        background: rgba(108,99,255,0.12);
        border-color: rgba(108,99,255,0.4);
        color: #3730a3;
    }
    .stat-box {
        display: flex;
        align-items: center;
        justify-content: space-between;
        background: linear-gradient(135deg, rgba(108,99,255,0.12), rgba(72,202,228,0.08));
        border: 1px solid rgba(108,99,255,0.25);
        border-radius: 12px;
        padding: 12px 16px;
        margin-top: 6px;
    }
    .stat-label { color: #6b7280; font-size: 0.78rem; font-weight: 500; }
    .stat-value { color: #5b21b6; font-size: 1.2rem; font-weight: 800; }

    /* ── Buttons ────────────────────────────────────────────────── */
    .stButton>button {
        background: linear-gradient(135deg, #6C63FF 0%, #48a8d8 100%) !important;
        color: white !important;
        border: none !important;
        border-radius: 12px !important;
        padding: 0.55rem 2rem !important;
        font-weight: 700 !important;
        font-size: 0.9rem !important;
        letter-spacing: 0.02em !important;
        box-shadow: 0 4px 20px rgba(108,99,255,0.35) !important;
        transition: all 0.25s ease !important;
        position: relative;
        overflow: hidden;
    }
    .stButton>button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 8px 30px rgba(108,99,255,0.5) !important;
        filter: brightness(1.1) !important;
    }
    .stButton>button:active { transform: translateY(0px) !important; }

    /* ── Tabs ───────────────────────────────────────────────────── */
    .stTabs [data-baseweb="tab-list"] {
        background: rgba(255,255,255,0.7);
        border-radius: 14px;
        padding: 4px;
        border: 1px solid rgba(108,99,255,0.12);
        gap: 4px;
    }
    .stTabs [data-baseweb="tab"] {
        border-radius: 10px !important;
        color: #6b7280 !important;
        font-weight: 600;
        font-size: 0.88rem;
        padding: 8px 20px !important;
        transition: all 0.2s;
    }
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, rgba(108,99,255,0.15), rgba(72,202,228,0.10)) !important;
        color: #4338ca !important;
        box-shadow: 0 2px 12px rgba(108,99,255,0.12) !important;
        border: 1px solid rgba(108,99,255,0.25) !important;
    }

    /* ── Text inputs ────────────────────────────────────────────── */
    /* Wrapper containers */
    .stTextInput,
    .stTextInput > div,
    .stTextInput > div > div {
        background: transparent !important;
    }
    .stTextInput > div > div > input {
        background: #ffffff !important;
        border: 1.5px solid rgba(108,99,255,0.25) !important;
        border-radius: 12px !important;
        color: #1e1b4b !important;
        padding: 14px 18px !important;
        font-size: 0.95rem !important;
        box-shadow: 0 2px 8px rgba(108,99,255,0.06) !important;
        transition: all 0.25s !important;
    }
    .stTextInput > div > div > input:focus,
    .stTextInput > div > div > input:focus-visible,
    .stTextInput > div[data-focused="true"] > div > input {
        background: #ffffff !important;
        border-color: #6C63FF !important;
        box-shadow: 0 0 0 3px rgba(108,99,255,0.12) !important;
        outline: none !important;
        color: #1e1b4b !important;
    }
    .stTextInput > div > div > input::placeholder { color: #9ca3af !important; }

    /* ── Dataframe ──────────────────────────────────────────────── */
    .stDataFrame {
        border-radius: 12px !important;
        overflow: hidden;
        border: 1px solid rgba(108,99,255,0.12) !important;
    }

    /* ── File uploader ──────────────────────────────────────────── */
    [data-testid="stFileUploader"] {
        background: rgba(255,255,255,0.03) !important;
        border: 2px dashed rgba(108,99,255,0.3) !important;
        border-radius: 14px !important;
        transition: border-color 0.3s;
    }
    [data-testid="stFileUploader"]:hover {
        border-color: rgba(108,99,255,0.6) !important;
    }

    /* ── Badges ─────────────────────────────────────────────────── */
    .badge {
        display: inline-flex;
        align-items: center;
        gap: 5px;
        padding: 4px 12px;
        border-radius: 30px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 6px;
        letter-spacing: 0.03em;
    }
    .badge-green  {
        background: rgba(16,185,129,0.10);
        color: #059669;
        border: 1px solid rgba(16,185,129,0.3);
    }
    .badge-purple {
        background: rgba(108,99,255,0.10);
        color: #6C63FF;
        border: 1px solid rgba(108,99,255,0.3);
    }

    /* ── Divider ────────────────────────────────────────────────── */
    hr {
        border: none;
        height: 1px;
        background: linear-gradient(90deg, transparent, rgba(108,99,255,0.4), transparent);
        margin: 20px 0;
    }

    /* ── Spinner & info ─────────────────────────────────────────── */
    .stSpinner>div { border-top-color: #6C63FF !important; }
    .stInfo    { background: rgba(72,202,228,0.08) !important; border: 1px solid rgba(72,202,228,0.25) !important; border-radius: 12px !important; }
    .stWarning { background: rgba(251,191,36,0.08) !important; border: 1px solid rgba(251,191,36,0.25) !important; border-radius: 12px !important; }
    .stSuccess { background: rgba(52,211,153,0.08) !important; border: 1px solid rgba(52,211,153,0.25) !important; border-radius: 12px !important; }
    .stError   { background: rgba(239,68,68,0.08)  !important; border: 1px solid rgba(239,68,68,0.25)  !important; border-radius: 12px !important; }
    </style>
    """, unsafe_allow_html=True)

    # Floating orbs (decorative)
    st.markdown("""
    <div class="orb orb-1"></div>
    <div class="orb orb-2"></div>
    <div class="orb orb-3"></div>
    """, unsafe_allow_html=True)

    # ── Init DB ───────────────────────────────────────────────────
    init_db()

    # ─────────────────────────────────────────
    # SIDEBAR — schema reference + sample Qs
    # ─────────────────────────────────────────
    with st.sidebar:
        # Logo header
        st.markdown("""
        <div class="sidebar-logo">
            <div class="sidebar-logo-icon">🤖</div>
            <div>
                <div class="sidebar-logo-text">Bantu</div>
                <div style="font-size:0.65rem;color:#475569;font-weight:500;letter-spacing:0.08em">THE DATA AGENT</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        # Database explorer
        st.markdown('<div class="sidebar-section-title">🗄️ Databases &amp; Tables</div>', unsafe_allow_html=True)

        explorer = get_db_explorer()
        if not explorer:
            st.caption("No .db files found.")
        else:
            for db_name, tables in explorer.items():
                # Build table rows first to avoid nested f-string issues
                rows_html = ""
                for t, cnt in tables:
                    cnt_str = f"{cnt:,}" if isinstance(cnt, int) else str(cnt)
                    rows_html += (
                        f'<div style="display:flex;align-items:center;justify-content:space-between;'
                        f'margin:5px 0 0 22px;padding:5px 10px;background:rgba(255,255,255,0.6);'
                        f'border-radius:7px;border:1px solid rgba(108,99,255,0.12);">'
                        f'<span style="font-size:0.78rem;color:#4338ca;font-weight:500;">&#9500;&#9472; {t}</span>'
                        f'<span style="font-size:0.7rem;background:rgba(108,99,255,0.1);color:#6C63FF;'
                        f'border-radius:20px;padding:1px 8px;font-weight:600;">{cnt_str} rows</span>'
                        f'</div>'
                    )
                card_html = (
                    f'<div style="background:linear-gradient(135deg,rgba(108,99,255,0.10),rgba(72,202,228,0.06));'
                    f'border:1px solid rgba(108,99,255,0.22);border-radius:10px;padding:9px 14px;margin-bottom:8px;">'
                    f'<div style="display:flex;align-items:center;gap:7px;margin-bottom:2px;">'
                    f'<span style="font-size:1rem;">&#128451;</span>'
                    f'<span style="font-size:0.82rem;font-weight:700;color:#3730a3;">{db_name}</span>'
                    f'</div>{rows_html}</div>'
                )
                st.markdown(card_html, unsafe_allow_html=True)

        st.markdown("<br>", unsafe_allow_html=True)


        # Sample questions
        st.markdown('<div class="sidebar-section-title">💡 Try asking</div>', unsafe_allow_html=True)
        sample_questions = [
            "Total sales by region",
            "Top 5 customers by amount",
            "Monthly revenue trend",
            "Average amount by region",
            "Show last 30 days data",
            "How many invoices by region?",
        ]
        
        # Override Streamlit button styles specifically for the sidebar sample questions
        st.markdown("""
        <style>
        [data-testid="stSidebar"] div.stButton > button {
            background-color: rgba(255,255,255,0.4);
            color: #3730A3;
            border: 1px solid rgba(108,99,255,0.2) !important;
            border-radius: 8px;
            padding: 4px 10px;
            font-size: 0.8rem;
            text-align: left;
            justify-content: flex-start;
            margin-bottom: -5px;
            font-weight: 500;
        }
        [data-testid="stSidebar"] div.stButton > button:hover {
            border: 1px solid rgba(108,99,255,0.6) !important;
            background-color: rgba(255,255,255,0.8);
            color: #4C1D95;
            box-shadow: none;
        }
        </style>
        """, unsafe_allow_html=True)

        for sq in sample_questions:
            if st.button(sq, key=f"btn_{sq}", use_container_width=True):
                st.session_state["query_input"] = sq
                st.rerun()

        st.markdown("<br>", unsafe_allow_html=True)

        # Live row count stat box
        con = sqlite3.connect(DB_PATH)
        row_count = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
        con.close()
        st.markdown(f"""
        <div class="stat-box">
            <div>
                <div class="stat-label">Records in DB</div>
                <div class="stat-value">{row_count:,}</div>
            </div>
            <div style="font-size:1.8rem">📊</div>
        </div>
        """, unsafe_allow_html=True)

    # ─────────────────────────────────────────
    # MAIN CONTENT
    # ─────────────────────────────────────────

    # Live stats for hero pills
    con = sqlite3.connect(DB_PATH)
    row_count_main = con.execute(f"SELECT COUNT(*) FROM {TABLE}").fetchone()[0]
    region_count   = con.execute(f"SELECT COUNT(DISTINCT region) FROM {TABLE}").fetchone()[0]
    customer_count = con.execute(f"SELECT COUNT(DISTINCT customer_name) FROM {TABLE}").fetchone()[0]
    con.close()

    st.markdown(f"""
    <div class="hero-wrap">
        <div class="hero-eyebrow">✦ The Data Agent</div>
        <h1 class="hero-title">Bantu</h1>
        <p style="text-align: center" class="hero-sub">Ask questions in plain English — get instant SQL, results &amp; charts</p>
        <div class="metric-row">
            <div class="metric-pill">
                <span class="metric-icon">📦</span>
                <div><div class="metric-val">{row_count_main:,}</div><div class="metric-lab">Records</div></div>
            </div>
            <div class="metric-pill">
                <span class="metric-icon">🌐</span>
                <div><div class="metric-val">{region_count}</div><div class="metric-lab">Regions</div></div>
            </div>
            <div class="metric-pill">
                <span class="metric-icon">🏢</span>
                <div><div class="metric-val">{customer_count}</div><div class="metric-lab">Customers</div></div>
            </div>
            <div class="metric-pill">
                <span class="metric-icon">⚡</span>
                <div><div class="metric-val">Live</div><div class="metric-lab">SQLite</div></div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Tab layout ────────────────────────────────────────────────
    tab_query, tab_upload = st.tabs(["🔍  Query", "📤  Upload Data"])

    # ══════════════════════════════════════════
    # TAB 1 — Natural Language Query
    # ══════════════════════════════════════════
    with tab_query:
        # ── Upload success toast ───────────────────────────────────
        if st.session_state.get("upload_toast"):
            toast_msg = st.session_state.pop("upload_toast")
            st.markdown(f"""
            <div style="background:linear-gradient(135deg,rgba(52,211,153,0.12),rgba(16,185,129,0.08));
                        border:1px solid rgba(52,211,153,0.35);border-radius:14px;
                        padding:16px 20px;margin-bottom:18px;
                        display:flex;align-items:flex-start;gap:14px;">
                <span style="font-size:1.6rem;line-height:1;">✅</span>
                <div>
                    <div style="font-size:0.9rem;font-weight:700;color:#065f46;margin-bottom:4px;">Successfully uploaded to database!</div>
                    <div style="font-size:0.82rem;color:#047857;">{toast_msg}</div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("""
        <div style="font-size:1.05rem;font-weight:700;color:#1e1b4b;
                    margin-bottom:14px;display:flex;align-items:center;gap:8px;">
            <span style="background:rgba(108,99,255,0.12);border:1px solid rgba(108,99,255,0.3);
                         border-radius:8px;padding:4px 8px;font-size:1rem;">💬</span>
            Ask a question about your data
        </div>
        """, unsafe_allow_html=True)

        # Initialize session state for the query input if not present
        if "query_input" not in st.session_state:
            st.session_state["query_input"] = ""

        # Natural language input bound to session_state key
        nl_input = st.text_input(
            label="Question",
            placeholder='e.g. "Total sales by region" or "Top 5 customers"',
            label_visibility="collapsed",
            key="query_input"  # binds strictly to st.session_state["query_input"]
        )

        col_btn, col_spacer = st.columns([1, 5])
        with col_btn:
            run_clicked = st.button("▶  Run Query", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        # ── Query execution ───────────────────────────────────────
        if run_clicked:
            if not nl_input.strip():
                st.warning("⚠️ Please enter a question before running.")
            else:
                with st.spinner("Generating SQL and fetching results…"):
                    # 1 — Generate SQL
                    generated_sql = sql_generator(nl_input)

                    # 2 — Show SQL
                    st.markdown("""
                    <div style='font-size:0.78rem;font-weight:700;letter-spacing:0.10em;
                                text-transform:uppercase;color:rgba(108,99,255,0.8);margin-bottom:8px'>
                        🛠️ &nbsp;Generated SQL
                    </div>
                    """, unsafe_allow_html=True)
                    st.markdown(
                        f'<div class="sql-chip">{generated_sql}</div>',
                        unsafe_allow_html=True,
                    )

                    # 3 — Execute
                    try:
                        result_df = run_query(generated_sql)
                    except Exception as exc:
                        st.error(f"❌ SQL Error: {exc}")
                        st.stop()

                    # 4 — Result table
                    st.markdown("""
                    <div style='font-size:0.78rem;font-weight:700;letter-spacing:0.10em;
                                text-transform:uppercase;color:rgba(52,211,153,0.8);margin:18px 0 8px'>
                        📋 &nbsp;Query Results
                    </div>
                    """, unsafe_allow_html=True)
                    if result_df.empty:
                        st.info("No rows returned.")
                    else:
                        st.markdown(
                            f"<span class='badge badge-green'>✓ {len(result_df)} rows</span>"
                            f"<span class='badge badge-purple'>{len(result_df.columns)} cols</span>",
                            unsafe_allow_html=True,
                        )
                        st.markdown("<br>", unsafe_allow_html=True)
                        st.dataframe(result_df, use_container_width=True, hide_index=True)

                        # 5 — Auto chart
                        fig = detect_chart(result_df)
                        if fig:
                            st.markdown("""
                            <div style='font-size:0.78rem;font-weight:700;letter-spacing:0.10em;
                                        text-transform:uppercase;color:rgba(168,85,247,0.8);margin:18px 0 8px'>
                                📈 &nbsp;Auto Visualization
                            </div>
                            """, unsafe_allow_html=True)
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.caption("ℹ️ No suitable chart — try a query with grouped or time-series data.")

    # ══════════════════════════════════════════
    # TAB 2 — Upload Excel
    # ══════════════════════════════════════════
    with tab_upload:
        if st.session_state.get("upload_success"):
            success_detail = st.session_state.pop("upload_success")

            # Live DB state — read fresh from disk
            live_explorer = get_db_explorer()

            # Build table rows
            table_rows_html = ""
            for db_name, tables in live_explorer.items():
                for t, cnt in tables:
                    cnt_str = f"{cnt:,}" if isinstance(cnt, int) else str(cnt)
                    table_rows_html += (
                        f'<div style="display:flex;justify-content:space-between;align-items:center;'
                        f'padding:8px 14px;border-radius:8px;margin-bottom:5px;'
                        f'background:rgba(255,255,255,0.7);border:1px solid rgba(108,99,255,0.12);">'
                        f'<span style="font-size:0.82rem;color:#3730a3;font-weight:600;">'
                        f'&#128451; {db_name} &nbsp;&#9656;&nbsp; {t}</span>'
                        f'<span style="font-size:0.75rem;background:rgba(108,99,255,0.1);color:#6C63FF;'
                        f'border-radius:20px;padding:2px 12px;font-weight:700;">{cnt_str} rows</span>'
                        f'</div>'
                    )

            st.markdown(f"""
            <div style="background:linear-gradient(135deg,rgba(52,211,153,0.10),rgba(16,185,129,0.06));
                        border:1.5px solid rgba(52,211,153,0.4);border-radius:16px;
                        padding:20px 24px;margin-bottom:20px;">
                <div style="display:flex;align-items:center;gap:12px;margin-bottom:10px;">
                    <span style="font-size:1.8rem">&#x2705;</span>
                    <span style="font-size:1rem;font-weight:700;color:#065f46;">Successfully uploaded to database!</span>
                </div>
                <div style="font-size:0.83rem;color:#047857;margin-left:2.8rem;line-height:1.8;margin-bottom:16px;">
                    {success_detail}
                </div>
                <div style="font-size:0.75rem;font-weight:700;letter-spacing:0.08em;text-transform:uppercase;
                            color:#374151;margin-bottom:8px;margin-left:2.8rem;">
                    &#128202; Current Database State
                </div>
                <div style="margin-left:2.8rem;">
                    {table_rows_html}
                </div>
            </div>
            """, unsafe_allow_html=True)

        st.markdown('<div class="glass-card">', unsafe_allow_html=True)
        st.markdown("#### 📤 Upload File to Append Data")
        st.markdown(
            f"Upload an `.xlsx`, `.csv`, `.pdf`, or `.docx` file. "
            f"Tabular data matching the `{TABLE}` schema will be appended. "
            "Different schemas or document files will automatically create a new table."
        )

        # Show expected columns
        expected = get_columns()
        st.markdown(f"**Expected columns (for {TABLE}):** `{'`, `'.join(expected)}`")

        uploaded_file = st.file_uploader(
            "Choose a file",
            type=["xlsx", "xls", "csv", "pdf", "docx", "doc"],
            help="Supported formats: Excel, CSV, PDF, Word Document.",
        )

        if uploaded_file:
            st.markdown("**Preview:**")
            try:
                ext = uploaded_file.name.lower()
                if ext.endswith(".csv"):
                    preview_df = pd.read_csv(uploaded_file, nrows=5)
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                elif ext.endswith((".pdf", ".docx", ".doc")):
                    st.info("Document file detected. Text will be extracted into a single 'extracted_text' column.")
                else:
                    preview_df = pd.read_excel(uploaded_file, nrows=5)
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                uploaded_file.seek(0)   # reset pointer for actual processing
            except Exception as e:
                st.error(f"Preview failed: {e}")

            if st.button("📥 Append to Database", use_container_width=False):
                with st.spinner("Validating and writing to database…"):
                    success, msg = append_uploaded_file(uploaded_file)
                if success:
                    # Increment refresh counter so sidebar MUST re-query
                    st.session_state["db_refresh"] = st.session_state.get("db_refresh", 0) + 1
                    st.session_state["upload_toast"] = msg
                    st.session_state["upload_success"] = msg
                    st.rerun()
                else:
                    st.error(msg)

        st.markdown("</div>", unsafe_allow_html=True)

        # ── Download template ─────────────────────────────────────
        st.markdown("<hr>", unsafe_allow_html=True)
        st.markdown("""
        <div class="card-header">
            <div class="card-header-icon">📎</div>
            <span>Download a starter template</span>
        </div>
        """, unsafe_allow_html=True)
        template_df = pd.DataFrame([{
            "invoice_id":    "INV-XXXX",
            "customer_name": "Example Corp",
            "amount":        12345.67,
            "invoice_date":  "2025-01-01",
            "region":        "North",
        }])
        st.download_button(
            label="⬇️  Download Excel Template",
            data=template_df.to_csv(index=False).encode(),
            file_name="askdata_template.csv",
            mime="text/csv",
        )


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────
if __name__ == "__main__":
    main()
