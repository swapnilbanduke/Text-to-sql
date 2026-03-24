"""
LangGraph workflow for orchestrating the Text-to-SQL pipeline.
"""

import re
from typing import Any, Optional, TypedDict

from langchain_anthropic import ChatAnthropic
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph

from database import get_sql_database, list_tables, run_query
from model_catalog import get_default_model, resolve_api_key
from prompts import get_human_answer_prompt, get_sql_generation_prompt


FORBIDDEN_KEYWORDS = [
    "INSERT",
    "UPDATE",
    "DELETE",
    "DROP",
    "ALTER",
    "TRUNCATE",
    "CREATE",
    "REPLACE",
    "GRANT",
    "REVOKE",
    "DENY",
    "EXEC",
    "EXECUTE",
]

MAX_RESULT_ROWS = 100


class WorkflowState(TypedDict):
    """Typed state for the text-to-SQL workflow."""

    question: str
    provider: str
    model_name: str
    api_key: Optional[str]
    tables: list[str]
    schema: str
    generated_sql: Optional[str]
    validated_sql: Optional[str]
    sql_result: Optional[list[Any]]
    final_answer: Optional[str]
    error: Optional[str]
    retry_count: int
    last_error_message: Optional[str]


def convert_error_to_friendly_message(error: str) -> str:
    """
    Convert technical errors into messages that are safe for end users.
    """
    error_lower = (error or "").lower()

    if any(token in error_lower for token in ["api key", "authentication", "unauthorized", "401"]):
        return "The selected AI provider rejected the API key. Please check the provider and key in the sidebar."
    if "rate limit" in error_lower or "quota" in error_lower:
        return "The selected AI provider is rate-limiting this request right now. Please wait a moment and try again."
    if "no such table" in error_lower:
        return "The database does not contain the table needed to answer that question."
    if "no such column" in error_lower or "unknown column" in error_lower:
        return "The database does not contain the column needed to answer that question."
    if "syntax" in error_lower or "parse" in error_lower:
        return "I could not generate a valid SQL query for that question. Please try rephrasing it."
    if "ambiguous" in error_lower:
        return "The question is ambiguous. Please be more specific about the field or table you want to analyze."
    if "timeout" in error_lower or "locked" in error_lower:
        return "The query took too long to execute. Please try a simpler question."

    return "I could not generate a valid SQL query for that question. The data may not contain the information you need."


def build_chat_model(provider: str, model_name: str, api_key: Optional[str]):
    """
    Create a chat model instance for the chosen provider.
    """
    provider_key = (provider or "").lower()
    resolved_model = model_name or get_default_model(provider_key)
    resolved_api_key = resolve_api_key(provider_key, api_key or "")

    if not resolved_api_key:
        raise ValueError("Missing API key for selected provider")

    if provider_key == "openai":
        return ChatOpenAI(
            model=resolved_model,
            temperature=0,
            timeout=60,
            api_key=resolved_api_key,
        )

    if provider_key == "anthropic":
        return ChatAnthropic(
            model_name=resolved_model,
            temperature=0,
            timeout=60,
            max_retries=2,
            api_key=resolved_api_key,
        )

    raise ValueError(f"Unsupported provider: {provider}")


def is_sql_safe(sql: str) -> tuple[bool, Optional[str]]:
    """
    Validate SQL for safety.
    """
    sql_upper = sql.strip().upper()

    if not sql_upper.startswith("SELECT"):
        return False, "Only SELECT queries are allowed."

    for keyword in FORBIDDEN_KEYWORDS:
        if keyword in sql_upper:
            return False, f"Query contains forbidden operation: {keyword}."

    if "--" in sql or "/*" in sql:
        return False, "SQL comments are not allowed."

    return True, None


def get_tables(state: WorkflowState) -> WorkflowState:
    """Load the list of available tables."""
    try:
        tables = list_tables()
        state["tables"] = tables

        if not tables:
            state["error"] = "No tables are available yet. Upload a CSV or Excel file first."
            return state

        print(f"[OK] Found {len(tables)} table(s): {', '.join(tables)}")
        return state
    except Exception as exc:
        state["error"] = f"Error retrieving tables: {exc}"
        return state


def get_schema(state: WorkflowState) -> WorkflowState:
    """Fetch schema information for prompt construction."""
    try:
        if state.get("error"):
            return state

        database = get_sql_database()
        state["schema"] = database.get_table_info()
        print("[OK] Schema retrieved successfully")
        return state
    except Exception as exc:
        state["error"] = f"Error retrieving schema: {exc}"
        return state


def generate_sql(state: WorkflowState) -> WorkflowState:
    """Generate SQL from the user's natural-language question."""
    try:
        if state.get("error"):
            return state

        llm = build_chat_model(state["provider"], state["model_name"], state.get("api_key"))
        prompt = get_sql_generation_prompt(state["question"], get_sql_database())

        if state.get("last_error_message") and state.get("retry_count", 0) > 0:
            prompt += f"\n\nPrevious attempt failed with this error: {state['last_error_message']}\n"
            prompt += "Generate a different SQL query that avoids this error.\n"

        response = llm.invoke(prompt)
        generated_sql = response.content.strip()
        generated_sql = re.sub(r"```sql\n?", "", generated_sql)
        generated_sql = re.sub(r"```\n?", "", generated_sql)
        generated_sql = generated_sql.strip()

        state["generated_sql"] = generated_sql
        state["last_error_message"] = None
        print(f"[OK] SQL generated with {state['provider']} / {state['model_name']}:\n{generated_sql}\n")
        return state
    except Exception as exc:
        state["error"] = f"Error generating SQL: {exc}"
        return state


def validate_sql(state: WorkflowState) -> WorkflowState:
    """Validate the generated SQL and enforce safe limits."""
    try:
        if state.get("error"):
            return state

        generated_sql = state.get("generated_sql")
        if not generated_sql:
            state["error"] = "No SQL was generated."
            return state

        is_safe, error_message = is_sql_safe(generated_sql)
        if not is_safe:
            if state.get("retry_count", 0) == 0:
                state["last_error_message"] = error_message
                state["retry_count"] = 1
                state["generated_sql"] = None
                print(f"[WARN] Validation failed, retrying: {error_message}")
                return state

            state["error"] = convert_error_to_friendly_message(error_message or "")
            return state

        validated_sql = generated_sql
        sql_upper = validated_sql.upper()

        if "LIMIT" not in sql_upper:
            validated_sql = f"{validated_sql} LIMIT {MAX_RESULT_ROWS}"
            print(f"[OK] Added LIMIT {MAX_RESULT_ROWS} to query")
        else:
            limit_match = re.search(r"LIMIT\s+(\d+)", sql_upper)
            if limit_match and int(limit_match.group(1)) > MAX_RESULT_ROWS:
                validated_sql = re.sub(
                    r"LIMIT\s+\d+",
                    f"LIMIT {MAX_RESULT_ROWS}",
                    validated_sql,
                    flags=re.IGNORECASE,
                )
                print(f"[OK] Limited results to {MAX_RESULT_ROWS} rows")

        state["validated_sql"] = validated_sql
        print("[OK] SQL validation passed")
        return state
    except Exception as exc:
        state["error"] = f"Error validating SQL: {exc}"
        return state


def execute_sql(state: WorkflowState) -> WorkflowState:
    """Execute validated SQL against the SQLite database."""
    try:
        if state.get("error"):
            return state

        validated_sql = state.get("validated_sql")
        if not validated_sql:
            state["error"] = "No validated SQL was available to execute."
            return state

        try:
            result = run_query(validated_sql)
            state["sql_result"] = result
            print(f"[OK] Query executed successfully. Returned {len(result)} row(s)")
            return state
        except Exception as exc:
            error_message = str(exc)

            if state.get("retry_count", 0) == 0:
                state["last_error_message"] = error_message
                state["retry_count"] = 1
                state["generated_sql"] = None
                state["validated_sql"] = None
                print(f"[WARN] Execution failed, retrying: {error_message}")
                return state

            state["error"] = convert_error_to_friendly_message(error_message)
            print(f"[ERROR] {state['error']}")
            return state
    except Exception as exc:
        state["error"] = convert_error_to_friendly_message(str(exc))
        return state


def summarize_answer(state: WorkflowState) -> WorkflowState:
    """Turn SQL results into a concise natural-language answer."""
    try:
        if state.get("error"):
            return state

        if state.get("sql_result") is None:
            state["error"] = "No query results were available to summarize."
            return state

        llm = build_chat_model(state["provider"], state["model_name"], state.get("api_key"))
        prompt = get_human_answer_prompt(
            question=state["question"],
            sql_query=state["validated_sql"] or "",
            sql_result=state["sql_result"],
        )
        response = llm.invoke(prompt)
        state["final_answer"] = response.content.strip()
        print(f"[OK] Answer summarized with {state['provider']} / {state['model_name']}")
        return state
    except Exception as exc:
        state["error"] = convert_error_to_friendly_message(str(exc))
        print(f"[ERROR] {state['error']}")
        return state


def handle_error(state: WorkflowState) -> WorkflowState:
    """Store the final friendly error message."""
    error_message = state.get("error") or "I could not process your question."
    if not error_message.startswith(("I could not", "The database", "No tables", "The selected AI provider")):
        error_message = convert_error_to_friendly_message(error_message)

    state["final_answer"] = error_message
    print(f"[ERROR] {error_message}")
    return state


def route_on_error(state: WorkflowState) -> str:
    """Route to the next graph node based on workflow state."""
    if state.get("error"):
        return "handle_error"

    if state.get("retry_count", 0) > 0 and state.get("generated_sql") is None:
        return "generate_sql"

    return "continue"


def route_after_generate(state: WorkflowState) -> str:
    return route_on_error(state)


def route_after_validate(state: WorkflowState) -> str:
    if state.get("error"):
        return "handle_error"
    if state.get("retry_count", 0) > 0 and state.get("generated_sql") is None:
        return "generate_sql"
    return "continue"


def route_after_execute(state: WorkflowState) -> str:
    if state.get("error"):
        return "handle_error"
    if state.get("retry_count", 0) > 0 and state.get("generated_sql") is None:
        return "generate_sql"
    return "continue"


def route_after_summarize(state: WorkflowState) -> str:
    return route_on_error(state)


def build_workflow() -> StateGraph:
    """Build the LangGraph workflow."""
    workflow = StateGraph(WorkflowState)

    workflow.add_node("get_tables", get_tables)
    workflow.add_node("get_schema", get_schema)
    workflow.add_node("generate_sql", generate_sql)
    workflow.add_node("validate_sql", validate_sql)
    workflow.add_node("execute_sql", execute_sql)
    workflow.add_node("summarize_answer", summarize_answer)
    workflow.add_node("handle_error", handle_error)

    workflow.add_edge("get_tables", "get_schema")
    workflow.add_edge("get_schema", "generate_sql")

    workflow.add_conditional_edges(
        "generate_sql",
        route_after_generate,
        {"handle_error": "handle_error", "continue": "validate_sql"},
    )
    workflow.add_conditional_edges(
        "validate_sql",
        route_after_validate,
        {"handle_error": "handle_error", "continue": "execute_sql", "generate_sql": "generate_sql"},
    )
    workflow.add_conditional_edges(
        "execute_sql",
        route_after_execute,
        {"handle_error": "handle_error", "continue": "summarize_answer", "generate_sql": "generate_sql"},
    )
    workflow.add_conditional_edges(
        "summarize_answer",
        route_after_summarize,
        {"handle_error": "handle_error", "continue": END},
    )

    workflow.add_edge("handle_error", END)
    workflow.set_entry_point("get_tables")
    return workflow.compile()


def process_question(
    question: str,
    provider: str = "openai",
    model_name: str = "",
    api_key: Optional[str] = None,
) -> dict:
    """
    Convert a natural-language question into SQL, execute it, and summarize the result.
    """
    provider_key = (provider or "openai").lower()
    resolved_model = model_name or get_default_model(provider_key)

    initial_state: WorkflowState = {
        "question": question,
        "provider": provider_key,
        "model_name": resolved_model,
        "api_key": api_key,
        "tables": [],
        "schema": "",
        "generated_sql": None,
        "validated_sql": None,
        "sql_result": None,
        "final_answer": None,
        "error": None,
        "retry_count": 0,
        "last_error_message": None,
    }

    final_state = build_workflow().invoke(initial_state)
    return {
        "answer": final_state.get("final_answer", "No answer generated"),
        "error": final_state.get("error"),
        "sql": final_state.get("validated_sql"),
        "result": final_state.get("sql_result"),
    }
