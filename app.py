"""
Professional Streamlit UI for the Text-to-SQL application.
"""

import os
from datetime import datetime
from hashlib import md5
from html import escape
from io import BytesIO

import pandas as pd
import streamlit as st

from database import drop_all_tables, run_query
from ingestion import ingest_csv, ingest_excel
from model_catalog import (
    DEFAULT_PROVIDER,
    get_default_model,
    get_model_label,
    get_model_values,
    get_provider_config,
    get_provider_ids,
    resolve_api_key,
    resolve_model,
)
from prompts import get_result_summary, normalize_sql_result
from workflow import process_question


st.set_page_config(
    page_title="Text-to-SQL Studio",
    page_icon=":material/query_stats:",
    layout="wide",
    initial_sidebar_state="expanded",
)


RESULT_TYPE_LABELS = {
    "empty": "Empty result",
    "single_number": "Single value",
    "single_row": "Single row",
    "grouped": "Grouped result",
    "wide": "Detailed table",
    "list": "Value list",
}


def inject_styles() -> None:
    """Add custom CSS for a more polished interface."""
    st.markdown(
        """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;500;600;700;800&family=IBM+Plex+Mono:wght@400;500&display=swap');

        :root {
            --bg: #f3f6fb;
            --ink: #10213a;
            --muted: #5e6d82;
            --line: rgba(16, 33, 58, 0.11);
            --surface: rgba(255, 255, 255, 0.88);
            --surface-strong: #ffffff;
            --surface-soft: #eef4fb;
            --accent: #2563eb;
            --accent-strong: #1d4ed8;
            --accent-soft: rgba(37, 99, 235, 0.10);
            --sidebar-bg: #e8eef6;
            --sidebar-panel: #ffffff;
            --sidebar-ink: #122033;
            --sidebar-muted: #5d6c80;
            --shadow: 0 22px 60px rgba(16, 33, 58, 0.08);
        }

        html, body, [class*="css"] {
            font-family: "Manrope", "Segoe UI", sans-serif;
            color: var(--ink);
        }

        code, pre, .stCode {
            font-family: "IBM Plex Mono", monospace !important;
        }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(circle at top left, rgba(37, 99, 235, 0.08), transparent 30%),
                radial-gradient(circle at top right, rgba(148, 163, 184, 0.12), transparent 28%),
                linear-gradient(180deg, #f8fbff 0%, #eef3f9 100%);
        }

        [data-testid="stSidebar"] {
            background:
                linear-gradient(180deg, #edf2f8 0%, #e6edf6 100%);
            border-right: 1px solid rgba(16, 33, 58, 0.08);
        }

        [data-testid="stSidebar"] .stMarkdown,
        [data-testid="stSidebar"] .stCaption,
        [data-testid="stSidebar"] p,
        [data-testid="stSidebar"] span,
        [data-testid="stSidebar"] div,
        [data-testid="stSidebar"] label,
        [data-testid="stSidebar"] h1,
        [data-testid="stSidebar"] h2,
        [data-testid="stSidebar"] h3 {
            color: var(--sidebar-ink);
        }

        [data-testid="stSidebar"] .stSelectbox label,
        [data-testid="stSidebar"] .stTextInput label,
        [data-testid="stSidebar"] .stRadio label {
            color: var(--sidebar-ink) !important;
            font-weight: 600;
        }

        [data-testid="stSidebar"] .stCaption,
        [data-testid="stSidebar"] p {
            color: var(--sidebar-muted) !important;
        }

        [data-testid="stSidebar"] .stAlert {
            background: rgba(255, 255, 255, 0.72);
            border: 1px solid rgba(16, 33, 58, 0.10);
            color: var(--sidebar-ink) !important;
        }

        h1, h2, h3 {
            color: var(--ink);
            letter-spacing: -0.02em;
        }

        .block-container {
            padding-top: 2rem;
            padding-bottom: 3rem;
        }

        .hero-shell {
            position: relative;
            overflow: hidden;
            padding: 2rem 2rem 1.8rem 2rem;
            border-radius: 28px;
            background:
                linear-gradient(135deg, #ffffff 0%, #f6f9ff 58%, #edf3ff 100%);
            color: var(--ink);
            box-shadow: var(--shadow);
            border: 1px solid rgba(37, 99, 235, 0.10);
            margin-bottom: 1rem;
        }

        .hero-shell::before {
            content: "";
            position: absolute;
            inset: 0 0 auto 0;
            height: 5px;
            background: linear-gradient(90deg, var(--accent) 0%, #60a5fa 100%);
        }

        .hero-shell::after {
            content: "";
            position: absolute;
            inset: auto -10% -35% auto;
            width: 340px;
            height: 340px;
            background: radial-gradient(circle, rgba(37, 99, 235, 0.13), transparent 68%);
            pointer-events: none;
        }

        .hero-eyebrow {
            display: inline-flex;
            align-items: center;
            gap: 0.45rem;
            padding: 0.38rem 0.7rem;
            border-radius: 999px;
            background: var(--accent-soft);
            border: 1px solid rgba(37, 99, 235, 0.14);
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: var(--accent-strong);
        }

        .hero-title {
            margin: 1rem 0 0.45rem 0;
            font-size: clamp(2.1rem, 4vw, 3.3rem);
            line-height: 1.02;
            font-weight: 800;
        }

        .hero-subtitle {
            max-width: 760px;
            margin: 0;
            color: var(--muted);
            font-size: 1rem;
            line-height: 1.7;
        }

        .hero-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.9rem;
            margin-top: 1.35rem;
        }

        .hero-stat {
            padding: 1rem 1rem 1.05rem 1rem;
            border-radius: 20px;
            background: rgba(255, 255, 255, 0.76);
            border: 1px solid rgba(16, 33, 58, 0.09);
            backdrop-filter: blur(6px);
        }

        .hero-stat span {
            display: block;
            font-size: 0.76rem;
            text-transform: uppercase;
            letter-spacing: 0.08em;
            color: var(--muted);
            margin-bottom: 0.45rem;
            font-weight: 700;
        }

        .hero-stat strong {
            display: block;
            font-size: 1.02rem;
            line-height: 1.35;
            color: var(--ink);
        }

        .section-copy {
            color: var(--muted);
            margin-bottom: 0.9rem;
        }

        .mini-note {
            padding: 0.9rem 1rem;
            border-radius: 18px;
            background: var(--accent-soft);
            border: 1px solid rgba(37, 99, 235, 0.14);
            color: var(--ink);
            margin-bottom: 1rem;
        }

        .empty-state {
            padding: 1.5rem;
            border-radius: 22px;
            background: var(--surface);
            border: 1px dashed rgba(17, 32, 51, 0.18);
            color: var(--muted);
            text-align: center;
        }

        .history-meta, .query-meta {
            color: var(--muted);
            font-size: 0.92rem;
        }

        [data-testid="stMetric"] {
            background: var(--surface);
            border: 1px solid var(--line);
            border-radius: 18px;
            padding: 1rem 1rem 0.9rem 1rem;
            box-shadow: 0 14px 30px rgba(17, 32, 51, 0.04);
        }

        [data-testid="stMetricLabel"] {
            font-weight: 700;
            color: var(--muted);
        }

        [data-testid="stMetricValue"] {
            color: var(--ink);
        }

        .stButton > button {
            border-radius: 14px;
            min-height: 3rem;
            font-weight: 700;
            border: 1px solid transparent;
            box-shadow: 0 10px 24px rgba(37, 99, 235, 0.16);
        }

        .stButton > button[kind="primary"] {
            background: linear-gradient(135deg, var(--accent) 0%, #3b82f6 100%);
        }

        .stDownloadButton > button {
            border-radius: 14px;
            border: 1px solid var(--line);
            background: var(--surface-strong);
            font-weight: 700;
        }

        .stTextInput input,
        .stTextArea textarea,
        .stSelectbox [data-baseweb="select"] > div {
            border-radius: 14px !important;
            border: 1px solid rgba(16, 33, 58, 0.12) !important;
            background: rgba(255, 255, 255, 0.96) !important;
            color: var(--ink) !important;
            box-shadow: none !important;
        }

        .stTextInput input::placeholder,
        .stTextArea textarea::placeholder {
            color: rgba(94, 109, 130, 0.82) !important;
        }

        [data-testid="stSidebar"] .stTextInput input,
        [data-testid="stSidebar"] .stTextArea textarea,
        [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] > div {
            background: var(--sidebar-panel) !important;
            color: var(--sidebar-ink) !important;
            border: 1px solid rgba(16, 33, 58, 0.14) !important;
        }

        [data-testid="stSidebar"] .stSelectbox [data-baseweb="select"] span,
        [data-testid="stSidebar"] .stTextInput input,
        [data-testid="stSidebar"] [data-baseweb="select"] * {
            color: var(--sidebar-ink) !important;
            fill: var(--sidebar-ink) !important;
        }

        [data-testid="stSidebar"] [data-baseweb="radio"] label,
        [data-testid="stSidebar"] [role="radiogroup"] label {
            color: var(--sidebar-ink) !important;
        }

        [data-baseweb="popover"],
        [data-baseweb="menu"] {
            background: #ffffff !important;
            border: 1px solid rgba(16, 33, 58, 0.12) !important;
            box-shadow: 0 16px 32px rgba(16, 33, 58, 0.14) !important;
            border-radius: 14px !important;
        }

        [data-baseweb="menu"] ul,
        [role="listbox"] {
            background: #ffffff !important;
        }

        [data-baseweb="menu"] li,
        [role="option"] {
            color: var(--ink) !important;
            background: #ffffff !important;
            font-weight: 600;
        }

        [data-baseweb="menu"] li:hover,
        [role="option"]:hover {
            background: rgba(37, 99, 235, 0.08) !important;
            color: var(--accent-strong) !important;
        }

        [aria-selected="true"][role="option"] {
            background: rgba(37, 99, 235, 0.12) !important;
            color: var(--accent-strong) !important;
        }

        [data-baseweb="tab-list"] {
            gap: 0.55rem;
        }

        [data-baseweb="tab"] {
            height: 3rem;
            border-radius: 999px;
            background: rgba(255, 255, 255, 0.92);
            padding: 0 1rem;
            font-weight: 700;
            color: var(--muted);
            border: 1px solid rgba(16, 33, 58, 0.10);
        }

        [data-baseweb="tab"][aria-selected="true"] {
            background: var(--accent);
            color: #ffffff !important;
            border-color: rgba(37, 99, 235, 0.28);
            box-shadow: 0 12px 24px rgba(37, 99, 235, 0.20);
        }

        [data-baseweb="tab"]:hover {
            color: var(--ink);
            border-color: rgba(37, 99, 235, 0.18);
        }

        @media (max-width: 900px) {
            .hero-shell {
                padding: 1.4rem;
            }

            .hero-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
        }

        @media (max-width: 640px) {
            .hero-grid {
                grid-template-columns: 1fr;
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def init_session_state() -> None:
    """Seed session state defaults."""
    defaults = {
        "uploaded_tables": {},
        "processed_uploads": set(),
        "chat_history": [],
        "latest_query": None,
        "question_input": "",
        "provider": DEFAULT_PROVIDER,
        "workspace_notice": None,
    }

    for key, value in defaults.items():
        if key not in st.session_state:
            if isinstance(value, dict):
                st.session_state[key] = {}
            elif isinstance(value, list):
                st.session_state[key] = []
            elif isinstance(value, set):
                st.session_state[key] = set()
            else:
                st.session_state[key] = value

    for provider_id in get_provider_ids():
        model_key = f"{provider_id}_selected_model"
        custom_key = f"{provider_id}_custom_model"
        api_key_state = f"{provider_id}_api_key"

        if model_key not in st.session_state:
            st.session_state[model_key] = get_default_model(provider_id)
        if custom_key not in st.session_state:
            st.session_state[custom_key] = ""
        if api_key_state not in st.session_state:
            st.session_state[api_key_state] = ""


def reset_workspace() -> None:
    """Clear the database and session data."""
    try:
        drop_all_tables()
        st.session_state.uploaded_tables = {}
        st.session_state.processed_uploads = set()
        st.session_state.chat_history = []
        st.session_state.latest_query = None
        st.session_state.question_input = ""
        st.session_state.workspace_notice = ("success", "Workspace reset successfully.")
    except Exception as exc:
        st.session_state.workspace_notice = ("error", f"Could not reset the workspace: {exc}")


def clear_prompt() -> None:
    """Clear the query input safely via widget callback."""
    st.session_state.question_input = ""


def sidebar_configuration() -> dict:
    """Render provider and API configuration in the sidebar."""
    with st.sidebar:
        st.markdown("## AI Control")
        st.caption("Choose the provider, model, and key used for both SQL generation and answer synthesis.")

        provider = st.radio(
            "Provider",
            options=get_provider_ids(),
            format_func=lambda value: get_provider_config(value)["label"],
            key="provider",
            horizontal=True,
        )

        provider_config = get_provider_config(provider)
        model_key = f"{provider}_selected_model"
        custom_model_key = f"{provider}_custom_model"
        api_key_state = f"{provider}_api_key"

        selected_model = st.selectbox(
            "Model",
            options=get_model_values(provider),
            format_func=lambda value: get_model_label(provider, value),
            key=model_key,
            help="Use a curated model from the list, or enter a custom model ID below.",
        )
        custom_model = st.text_input(
            "Custom model ID (optional)",
            key=custom_model_key,
            placeholder="Leave blank to use the selected model",
        )
        active_model = resolve_model(provider, selected_model, custom_model)

        api_key_input = st.text_input(
            provider_config["api_key_label"],
            type="password",
            key=api_key_state,
            help=provider_config["api_key_help"],
        )
        if api_key_input:
            os.environ[provider_config["env_var"]] = api_key_input

        active_api_key = resolve_api_key(provider, st.session_state.get(api_key_state, ""))

        if st.session_state.get(api_key_state):
            st.success("API key loaded for this session.")
        elif os.getenv(provider_config["env_var"]):
            st.info(f"Using {provider_config['env_var']} from the environment.")
        else:
            st.warning(f"No {provider_config['label']} API key detected.")

        st.caption(provider_config["description"])

        with st.container(border=True):
            st.markdown("### Active setup")
            st.markdown(
                f"""
                <div class="query-meta">
                    Provider: <strong>{escape(provider_config["label"])}</strong><br/>
                    Model: <strong>{escape(active_model)}</strong><br/>
                    Auth: <strong>{"Ready" if active_api_key else "Missing key"}</strong>
                </div>
                """,
                unsafe_allow_html=True,
            )

        st.markdown("### Workspace")
        st.button("Reset workspace", width="stretch", on_click=reset_workspace)

        return {
            "provider": provider,
            "provider_label": provider_config["label"],
            "model_name": active_model,
            "model_label": get_model_label(provider, selected_model) if not custom_model.strip() else active_model,
            "api_key": active_api_key,
            "has_api_key": bool(active_api_key),
        }


def render_hero(ai_config: dict) -> None:
    """Render the top-of-page hero section."""
    total_tables = len(st.session_state.uploaded_tables)
    total_rows = sum(metadata["rows"] for metadata in st.session_state.uploaded_tables.values())

    st.markdown(
        f"""
        <div class="hero-shell">
            <div class="hero-eyebrow">Professional Text-to-SQL Studio</div>
            <div class="hero-title">Query structured data with the model you trust.</div>
            <p class="hero-subtitle">
                Upload CSV or Excel files, choose between OpenAI and Anthropic models, and turn business questions into governed SQL with a cleaner analyst workflow.
            </p>
            <div class="hero-grid">
                <div class="hero-stat">
                    <span>Provider</span>
                    <strong>{escape(ai_config["provider_label"])}</strong>
                </div>
                <div class="hero-stat">
                    <span>Model</span>
                    <strong>{escape(ai_config["model_label"])}</strong>
                </div>
                <div class="hero-stat">
                    <span>Tables</span>
                    <strong>{total_tables}</strong>
                </div>
                <div class="hero-stat">
                    <span>Rows Available</span>
                    <strong>{total_rows:,}</strong>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def process_uploaded_files(uploaded_files) -> None:
    """Load uploaded files into the database once per session."""
    if not uploaded_files:
        return

    for uploaded_file in uploaded_files:
        file_bytes = uploaded_file.getvalue()
        file_signature = f"{uploaded_file.name}:{md5(file_bytes).hexdigest()}"

        if file_signature in st.session_state.processed_uploads:
            continue

        try:
            if uploaded_file.name.lower().endswith(".csv"):
                report = ingest_csv(BytesIO(file_bytes), table_name=uploaded_file.name.rsplit(".", 1)[0])
                st.session_state.uploaded_tables[report["table_name"]] = {
                    "original_name": uploaded_file.name,
                    "type": "CSV",
                    "rows": report["rows"],
                    "columns": report["columns"],
                    "column_mapping": report["column_mapping"],
                }
                st.success(f"Loaded {report['rows']:,} rows into `{report['table_name']}`.")

            elif uploaded_file.name.lower().endswith((".xlsx", ".xls")):
                report = ingest_excel(BytesIO(file_bytes))
                for table_info in report["tables"]:
                    st.session_state.uploaded_tables[table_info["sql_table_name"]] = {
                        "original_sheet": table_info["original_sheet_name"],
                        "type": "Excel",
                        "rows": table_info["rows"],
                        "columns": table_info["columns"],
                        "column_mapping": table_info["column_mapping"],
                    }
                    st.success(
                        f"Loaded sheet `{table_info['original_sheet_name']}` as `{table_info['sql_table_name']}` with {table_info['rows']:,} rows."
                    )

            st.session_state.processed_uploads.add(file_signature)
        except Exception as exc:
            st.error(f"Error processing `{uploaded_file.name}`: {exc}")


def get_preview_dataframe(table_name: str, metadata: dict) -> pd.DataFrame:
    """Return a preview DataFrame for a table."""
    preview_rows = run_query(f"SELECT * FROM [{table_name}] LIMIT 5")
    normalized_preview = normalize_sql_result(preview_rows)
    preview_df = pd.DataFrame(normalized_preview)

    expected_columns = metadata.get("columns", [])
    if expected_columns and len(preview_df.columns) == len(expected_columns):
        preview_df.columns = expected_columns

    return preview_df


def result_summary_text(summary: dict) -> str:
    """Create a UI-friendly result summary label."""
    result_type = RESULT_TYPE_LABELS.get(summary["result_type"], "Query result")
    return f"{result_type} | {summary['num_rows']} row(s) x {summary['num_cols']} column(s)"


def run_query_workflow(question: str, ai_config: dict) -> None:
    """Execute the natural-language query and store the latest result."""
    question_text = (question or "").strip()

    if not question_text:
        st.warning("Enter a question before running the query.")
        return

    if not ai_config["has_api_key"]:
        st.error(f"Add a {ai_config['provider_label']} API key in the sidebar before running a query.")
        return

    with st.spinner(f"Running analysis with {ai_config['provider_label']} / {ai_config['model_label']}..."):
        try:
            result = process_question(
                question_text,
                provider=ai_config["provider"],
                model_name=ai_config["model_name"],
                api_key=ai_config["api_key"],
            )
            normalized_result = normalize_sql_result(result.get("result") or [])
            summary = get_result_summary(normalized_result)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")

            latest_query = {
                "question": question_text,
                "provider": ai_config["provider_label"],
                "model_name": ai_config["model_name"],
                "sql": result.get("sql"),
                "answer": result.get("answer"),
                "error": result.get("error"),
                "result": normalized_result,
                "summary": summary,
                "timestamp": timestamp,
            }

            st.session_state.latest_query = latest_query
            st.session_state.chat_history.insert(0, latest_query)

            if latest_query["error"]:
                st.warning("The workflow completed with a partial or fallback response.")
            else:
                st.success("Query completed successfully.")
        except Exception:
            st.error("An unexpected error occurred while processing the question.")
            st.caption("Try rephrasing the request or review the selected provider and model.")


def render_data_workspace(ai_config: dict) -> None:
    """Render file upload, data overview, and schema preview."""
    st.markdown("### Data workspace")
    st.markdown(
        '<div class="section-copy">Bring in one or more CSV or Excel files. Each Excel sheet becomes its own SQL table automatically.</div>',
        unsafe_allow_html=True,
    )

    with st.container(border=True):
        uploaded_files = st.file_uploader(
            "Upload CSV or Excel files",
            type=["csv", "xls", "xlsx"],
            accept_multiple_files=True,
            help="You can upload multiple files in one pass.",
        )
        process_uploaded_files(uploaded_files)

    if not st.session_state.uploaded_tables:
        st.markdown(
            """
            <div class="empty-state">
                <strong>No data has been loaded yet.</strong><br/>
                Start by uploading a CSV or Excel workbook to unlock previews, schema inspection, and natural-language querying.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    total_rows = sum(metadata["rows"] for metadata in st.session_state.uploaded_tables.values())
    metric_cols = st.columns(4)
    metric_cols[0].metric("Tables", len(st.session_state.uploaded_tables))
    metric_cols[1].metric("Rows", f"{total_rows:,}")
    metric_cols[2].metric("Provider", ai_config["provider_label"])
    metric_cols[3].metric("Model", ai_config["model_label"])

    st.markdown("### Table preview")
    for table_name, metadata in st.session_state.uploaded_tables.items():
        with st.expander(f"{table_name} | {metadata['rows']:,} rows | {metadata['type']}", expanded=False):
            try:
                preview_df = get_preview_dataframe(table_name, metadata)
                st.dataframe(preview_df, width="stretch", hide_index=True)
            except Exception as exc:
                st.warning(f"Preview unavailable for `{table_name}`: {exc}")

            st.markdown(
                f"""
                <div class="query-meta">
                    Source: {escape(metadata.get("original_name", metadata.get("original_sheet", "Unknown")))}<br/>
                    Rows: {metadata["rows"]:,}<br/>
                    Columns: {len(metadata["columns"])}
                </div>
                """,
                unsafe_allow_html=True,
            )

            mapping_df = pd.DataFrame(
                [
                    {"Original column": original, "SQL column": cleaned}
                    for original, cleaned in metadata["column_mapping"].items()
                ]
            )
            st.dataframe(mapping_df, width="stretch", hide_index=True)


def render_latest_query() -> None:
    """Render the most recent query result."""
    latest_query = st.session_state.latest_query
    if not latest_query:
        st.markdown(
            """
            <div class="empty-state">
                <strong>No query has been run yet.</strong><br/>
                Use the query studio to ask a business question once your tables are loaded.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    summary = latest_query["summary"]
    summary_cols = st.columns(4)
    summary_cols[0].metric("Result type", RESULT_TYPE_LABELS.get(summary["result_type"], "Query result"))
    summary_cols[1].metric("Rows returned", summary["num_rows"])
    summary_cols[2].metric("Columns", summary["num_cols"])
    summary_cols[3].metric("Run at", latest_query["timestamp"])

    top_left, top_right = st.columns([1.35, 1])

    with top_left:
        with st.container(border=True):
            st.markdown("### Analyst answer")
            st.caption(
                f"Question: {latest_query['question']} | Provider: {latest_query['provider']} | Model: {latest_query['model_name']}"
            )
            if latest_query["error"]:
                st.warning(latest_query["answer"])
            else:
                st.write(latest_query["answer"])

    with top_right:
        with st.container(border=True):
            st.markdown("### Generated SQL")
            if latest_query["sql"]:
                st.code(latest_query["sql"], language="sql")
            else:
                st.write("No SQL was generated for this run.")

    with st.container(border=True):
        st.markdown("### Result table")
        st.caption(result_summary_text(summary))
        if latest_query["result"]:
            result_df = pd.DataFrame(latest_query["result"])
            st.dataframe(result_df, width="stretch", hide_index=True)
            st.download_button(
                "Download result as CSV",
                data=result_df.to_csv(index=False).encode("utf-8"),
                file_name="text_to_sql_result.csv",
                mime="text/csv",
                width="stretch",
            )
        else:
            st.info("This query did not return any rows.")


def render_query_studio(ai_config: dict) -> None:
    """Render the natural-language querying area."""
    st.markdown("### Query studio")
    st.markdown(
        '<div class="mini-note">The selected provider and model will be used for both SQL generation and the final natural-language explanation.</div>',
        unsafe_allow_html=True,
    )

    if not st.session_state.uploaded_tables:
        st.markdown(
            """
            <div class="empty-state">
                <strong>Upload data before asking a question.</strong><br/>
                The query studio becomes active as soon as at least one table is available.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    with st.container(border=True):
        st.text_area(
            "Ask a question about your data",
            key="question_input",
            height=120,
            placeholder="Examples: How many countries are in the occupazione table? What is the average value by year? Show the top 10 rows by budget.",
            help="Write the business question naturally. The app will convert it to SQL.",
        )
        button_cols = st.columns([1.2, 1, 2.8])
        run_clicked = button_cols[0].button("Run query", type="primary", width="stretch")
        button_cols[1].button("Clear prompt", width="stretch", on_click=clear_prompt)
        button_cols[2].caption(
            f"Active model: {ai_config['provider_label']} / {ai_config['model_label']}"
        )

        if run_clicked:
            run_query_workflow(st.session_state.question_input, ai_config)

    st.markdown("### Latest result")
    render_latest_query()


def render_history() -> None:
    """Render prior queries for the session."""
    st.markdown("### Query history")
    if not st.session_state.chat_history:
        st.markdown(
            """
            <div class="empty-state">
                <strong>No history yet.</strong><br/>
                Each successful query will appear here with the provider, model, SQL, and result summary.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    st.caption(f"{len(st.session_state.chat_history)} query run(s) in this session.")

    for index, entry in enumerate(st.session_state.chat_history, start=1):
        with st.expander(f"{index}. {entry['question']}", expanded=index == 1):
            st.markdown(
                f"""
                <div class="history-meta">
                    {entry['timestamp']} | Provider: {entry['provider']} | Model: {escape(entry['model_name'])}
                </div>
                """,
                unsafe_allow_html=True,
            )
            if entry["error"]:
                st.warning(entry["answer"])
            else:
                st.write(entry["answer"])

            if entry["sql"]:
                st.code(entry["sql"], language="sql")

            if entry["result"]:
                history_df = pd.DataFrame(entry["result"])
                st.dataframe(history_df, width="stretch", hide_index=True)
            else:
                st.info("No rows returned for this query.")


def main() -> None:
    """Run the Streamlit application."""
    inject_styles()
    init_session_state()

    workspace_notice = st.session_state.pop("workspace_notice", None)
    if workspace_notice:
        level, message = workspace_notice
        if level == "success":
            st.success(message)
        else:
            st.error(message)

    ai_config = sidebar_configuration()
    render_hero(ai_config)

    tabs = st.tabs(["Data", "Query", "History"])
    with tabs[0]:
        render_data_workspace(ai_config)
    with tabs[1]:
        render_query_studio(ai_config)
    with tabs[2]:
        render_history()


if __name__ == "__main__":
    main()
