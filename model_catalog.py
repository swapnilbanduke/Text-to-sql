"""
Shared provider and model configuration for the Text-to-SQL app.
"""

import os


DEFAULT_PROVIDER = "openai"


PROVIDER_CATALOG = {
    "openai": {
        "label": "OpenAI",
        "env_var": "OPENAI_API_KEY",
        "api_key_label": "OpenAI API key",
        "api_key_help": "Used for GPT models such as GPT-5 and GPT-4.1.",
        "description": "Strong default choice for SQL generation and concise summaries.",
        "models": [
            {"value": "gpt-5", "label": "GPT-5"},
            {"value": "gpt-5-mini", "label": "GPT-5 mini"},
            {"value": "gpt-4.1", "label": "GPT-4.1"},
            {"value": "gpt-4.1-mini", "label": "GPT-4.1 mini"},
            {"value": "gpt-4o", "label": "GPT-4o"},
        ],
        "default_model": "gpt-5-mini",
    },
    "anthropic": {
        "label": "Anthropic",
        "env_var": "ANTHROPIC_API_KEY",
        "api_key_label": "Anthropic API key",
        "api_key_help": "Used for Claude models such as Sonnet and Opus.",
        "description": "Claude models are often strong on schema reasoning and nuanced explanations.",
        "models": [
            {"value": "claude-sonnet-4-20250514", "label": "Claude Sonnet 4"},
            {"value": "claude-opus-4-20250514", "label": "Claude Opus 4"},
            {"value": "claude-3-7-sonnet-20250219", "label": "Claude Sonnet 3.7"},
            {"value": "claude-3-5-haiku-20241022", "label": "Claude Haiku 3.5"},
        ],
        "default_model": "claude-sonnet-4-20250514",
    },
}


def get_provider_ids() -> list[str]:
    """Return supported provider IDs."""
    return list(PROVIDER_CATALOG.keys())


def get_provider_config(provider: str) -> dict:
    """Return configuration for a provider."""
    provider_key = (provider or "").lower()
    if provider_key not in PROVIDER_CATALOG:
        raise ValueError(f"Unsupported provider: {provider}")
    return PROVIDER_CATALOG[provider_key]


def get_model_options(provider: str) -> list[dict]:
    """Return model option dictionaries for a provider."""
    return get_provider_config(provider)["models"]


def get_model_values(provider: str) -> list[str]:
    """Return model IDs for a provider."""
    return [model["value"] for model in get_model_options(provider)]


def get_model_label(provider: str, model_value: str) -> str:
    """Return the display label for a model."""
    for model in get_model_options(provider):
        if model["value"] == model_value:
            return model["label"]
    return model_value


def get_default_model(provider: str) -> str:
    """Return the default model for a provider."""
    return get_provider_config(provider)["default_model"]


def resolve_model(provider: str, selected_model: str, custom_model: str = "") -> str:
    """Prefer a custom model if supplied, otherwise use the selected model."""
    custom_value = (custom_model or "").strip()
    if custom_value:
        return custom_value

    if selected_model:
        return selected_model

    return get_default_model(provider)


def resolve_api_key(provider: str, session_value: str = "") -> str:
    """Resolve an API key from session input or environment variables."""
    config = get_provider_config(provider)
    return (session_value or "").strip() or os.getenv(config["env_var"], "")
