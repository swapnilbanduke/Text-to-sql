# Text-to-SQL Studio

Professional Streamlit app for querying uploaded CSV and Excel files with either OpenAI or Anthropic models.

## Highlights

- Provider switcher for OpenAI and Anthropic
- Curated model picker with optional custom model ID override
- Upgraded studio-style UI with dedicated Data, Query, and History tabs
- Read-only SQL generation with result normalization for safer rendering

## Run locally

```bash
cd text_to_sql_app
pip install -r requirements.txt
streamlit run app.py
```

Set either `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` in the environment, or enter the key in the sidebar at runtime.

## Core files

- `app.py`: professional Streamlit interface
- `workflow.py`: provider-aware LangGraph pipeline
- `model_catalog.py`: shared provider and model configuration
- `database.py`: SQLite helpers
- `ingestion.py`: CSV and Excel ingestion
- `prompts.py`: prompt templates and result normalization
- `.env.example`: provider-specific environment variables

## Notes

- Only `SELECT` queries are allowed.
- Excel uploads are split into one SQL table per sheet.
- Query results are normalized before display so SQLAlchemy row objects render cleanly in the UI.
