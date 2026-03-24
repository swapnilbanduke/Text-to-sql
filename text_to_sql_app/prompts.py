"""
Prompt templates for LLM interactions to generate SQL queries and format results.
"""

from langchain_community.utilities import SQLDatabase


# ==================== Result Type Detection ====================

def row_to_dict(row):
    """
    Convert a result row to a plain dictionary when possible.

    Supports SQLAlchemy Row objects, RowMapping, and dict-like rows.
    Returns None for tuple/list/scalar rows.
    """
    if row is None:
        return None

    if isinstance(row, dict):
        return dict(row)

    mapping = getattr(row, "_mapping", None)
    if mapping is not None:
        return dict(mapping)

    if hasattr(row, "keys"):
        try:
            return {key: row[key] for key in row.keys()}
        except Exception:
            try:
                return dict(row)
            except Exception:
                return None

    return None


def row_to_values(row) -> list:
    """
    Convert a result row to a plain list of values.
    """
    row_dict = row_to_dict(row)
    if row_dict is not None:
        return list(row_dict.values())

    if isinstance(row, tuple):
        return list(row)

    if isinstance(row, list):
        return row

    try:
        return list(row)
    except TypeError:
        return [row]


def row_column_names(row) -> list:
    """
    Get column names for a result row when available.
    Falls back to generated column names for tuple-like rows.
    """
    row_dict = row_to_dict(row)
    if row_dict is not None:
        return list(row_dict.keys())

    return [f"col_{i+1}" for i in range(len(row_to_values(row)))]


def normalize_sql_result(sql_result: list) -> list:
    """
    Normalize SQL results into plain Python structures for UI display.

    Dict-like rows stay dicts. Tuple-like rows become tuples.
    """
    normalized_rows = []

    for row in sql_result or []:
        row_dict = row_to_dict(row)
        if row_dict is not None:
            normalized_rows.append(row_dict)
        else:
            normalized_rows.append(tuple(row_to_values(row)))

    return normalized_rows

def detect_result_type(sql_result: list) -> tuple:
    """
    Detect the type of SQL result to guide humanization.
    
    Returns:
        tuple: (result_type, analysis_dict)
        - result_type: 'empty', 'single_number', 'grouped', 'wide', 'list'
        - analysis_dict: Contains details about the result structure
    """
    if not sql_result or len(sql_result) == 0:
        return 'empty', {'rows': 0}
    
    # Analyze first row
    first_row = sql_result[0]
    first_values = row_to_values(first_row)
    col_names = row_column_names(first_row)
    
    # Count columns
    num_cols = len(first_values)
    
    num_rows = len(sql_result)
    
    # Single number: 1 row, 1 column with numeric value
    if num_rows == 1 and num_cols == 1:
        value = first_values[0]
        try:
            float(value)  # Check if numeric
            return 'single_number', {
                'value': value,
                'col_name': col_names[0],
                'rows': 1,
                'cols': 1
            }
        except (ValueError, TypeError):
            pass

    # Single row with multiple columns
    if num_rows == 1 and num_cols > 1:
        return 'single_row', {
            'rows': 1,
            'cols': num_cols,
            'col_names': col_names
        }
    
    # Wide result: More than 4 columns (shows summary with detailed rows below)
    if num_cols > 4:
        return 'wide', {
            'rows': num_rows,
            'cols': num_cols,
            'col_names': col_names
        }
    
    # Grouped result: Multiple rows, few columns (likely aggregated/grouped data)
    if num_rows > 1 and num_cols <= 4:
        return 'grouped', {
            'rows': num_rows,
            'cols': num_cols,
            'col_names': col_names
        }
    
    # Single list result: 1 column, multiple rows
    if num_cols == 1 and num_rows > 1:
        return 'list', {
            'rows': num_rows,
            'col_name': col_names[0],
            'values': [row_to_values(row)[0] for row in sql_result if row_to_values(row)]
        }
    
    return 'list', {
        'rows': num_rows,
        'cols': num_cols,
        'col_names': col_names
    }


# ==================== Prompt Functions ====================


def get_sql_generation_prompt(question: str, database: SQLDatabase) -> str:
    """
    Generate a prompt for SQL query generation from natural language.
    
    Includes:
    - Available tables and schema information
    - User's natural language question
    - Instructions for generating valid SQL
    - Best practices (aggregation, grouping, LIMIT, etc.)
    
    Args:
        question (str): User's natural language question
        database (SQLDatabase): LangChain SQLDatabase instance
        
    Returns:
        str: Complete prompt for LLM
    """
    
    # Get database schema information
    table_info = database.get_table_info()
    
    prompt = f"""You are an expert SQL developer. Your task is to generate a SQL query based on the user's question.

## Available Database Schema

{table_info}

## Instructions

1. **Use Only SQL**: Generate ONLY a valid SQL SELECT query. No explanations, no markdown, no code blocks.

2. **Correct Names**: Use EXACTLY the table names and column names from the schema above. Do not invent or guess column names.

3. **Aggregation & Grouping**: When the user asks for summaries, totals, counts, or comparisons:
   - Use GROUP BY for categorical grouping
   - Use appropriate aggregate functions: COUNT(), SUM(), AVG(), MIN(), MAX()
   - Include HAVING clauses for filtered aggregations

4. **Limit Results**: Always add LIMIT 100 unless the user specifically asks for all results. This prevents returning too much data.

5. **Read-Only**: Generate ONLY SELECT queries. Never generate INSERT, UPDATE, DELETE, or CREATE statements.

6. **Output Format**: Output ONLY the SQL query. No explanation. No code block markers. Just the pure SQL.

## User Question

{question}

Generate the SQL query now:"""

    return prompt


def get_human_answer_prompt(
    question: str,
    sql_query: str,
    sql_result: list
) -> str:
    """
    Generate a prompt for formatting SQL results into business-friendly language.
    
    Intelligently handles different result types:
    - Single number: Answer directly (e.g., "The total is 15,234")
    - Grouped rows: Summarize key findings
    - Empty results: Say no matching records found
    - Wide results: Highlight top findings, mention detailed rows below
    
    Args:
        question (str): Original user's question
        sql_query (str): SQL query that was executed
        sql_result (list): Raw results from SQL query
        
    Returns:
        str: Complete prompt for LLM
    """
    
    # Detect result type for smarter handling
    result_type, analysis = detect_result_type(sql_result)
    
    # Format results for readability based on type
    if result_type == 'empty':
        result_str = "No data found: The query returned zero results."
        context_instruction = """
## Important: Empty Result Context
The query returned no matching records. This means either:
- There are no records matching the user's criteria
- The data might not contain what they're looking for
- The question might need different parameters

Politely explain that no matching records were found and suggest what they might try instead."""
    
    elif result_type == 'single_number':
        value = analysis['value']
        col_name = analysis['col_name']
        result_str = f"Result: {col_name} = {value}"
        context_instruction = f"""
## Important: Single Value Result
The query returned exactly ONE number: {value}

This is the direct answer to the user's question. State this number clearly and directly in plain English.
Use formatting like "The total is 15,234" or "We found 42 results" - speak like an analyst, not like a query tool."""
    
    elif result_type == 'single_row':
        row_dict = row_to_dict(sql_result[0])
        if row_dict is not None:
            result_str = "Result: " + str(row_dict)
        else:
            result_str = "Result: " + str(sql_result[0])
        context_instruction = "The query returned exactly ONE row. Explain what this single result means in context."
    
    elif result_type == 'grouped':
        # Show aggregated/grouped data with key findings
        result_str = f"Results ({analysis['rows']} groups/rows):\n"
        for i, row in enumerate(sql_result[:10]):
            row_dict = row_to_dict(row)
            if row_dict is not None:
                result_str += f"Row {i+1}: {row_dict}\n"
            else:
                result_str += f"Row {i+1}: {row}\n"
        
        if analysis['rows'] > 10:
            result_str += f"\n... and {analysis['rows'] - 10} more groups (total: {analysis['rows']} rows)"
        
        context_instruction = f"""
## Important: Grouped/Aggregated Data
This is grouped or summarized data with {analysis['rows']} different groups/categories.

Instructions:
1. **Summarize Key Findings**: Mention the top 2-3 most important groups or patterns
2. **Highlight Extremes**: Point out the highest, lowest, or most significant values
3. **Quantify**: Use specific numbers and comparisons (e.g., "Top group has 500, lowest has 50")
4. **Brief**: Keep it to 2-3 sentences max. Detailed rows are shown below for exploration"""
    
    elif result_type == 'wide':
        # Many columns - show summary and mention detailed view
        result_str = f"Results ({analysis['rows']} rows, {analysis['cols']} columns):\n"
        for i, row in enumerate(sql_result[:5]):  # Show fewer rows for wide results
            row_dict = row_to_dict(row)
            if row_dict is not None:
                result_str += f"Row {i+1}: {row_dict}\n"
            else:
                result_str += f"Row {i+1}: {row}\n"
        
        if analysis['rows'] > 5:
            result_str += f"\n... and {analysis['rows'] - 5} more rows"
        
        context_instruction = f"""
## Important: Wide Dataset (Many Columns)
This result has many columns ({analysis['cols']}) which gives a detailed view.

Instructions:
1. **Summarize Top Row**: Describe the key findings from the first row(s)
2. **Highlight Patterns**: Mention any obvious trends or notable values
3. **Mention Detail**: Say something like "The full details are shown below with {analysis['cols']} data points"
4. **Stay High-Level**: Keep your summary to 2-3 sentences. Users can explore detailed columns below"""
    
    elif result_type == 'list':
        # List of values - aggregate intelligently
        values = analysis.get('values', [])
        result_str = f"Results (list of {analysis['rows']} values):\n"
        for i, val in enumerate(values[:15]):
            result_str += f"- {val}\n"
        
        if analysis['rows'] > 15:
            result_str += f"\n... and {analysis['rows'] - 15} more"
        
        context_instruction = f"""
## Important: List of Values
This is a list of {analysis['rows']} values from the column '{analysis['col_name']}'.

Instructions:
1. **Aggregate**: Count, find patterns, or group similar values
2. **Highlight**: Point out the most frequent, most important, or most interesting items
3. **Summarize**: Say something like "We found {analysis['rows']} total {analysis['col_name'].lower()}, with X being the most common"
4. **Be Conversational**: Talk to them like an analyst reviewing findings, not like a query tool"""
    
    else:
        # Default: Show first 10 rows
        result_str = "Results:\n"
        if isinstance(sql_result, list) and len(sql_result) > 0:
            for i, row in enumerate(sql_result[:10]):
                row_dict = row_to_dict(row)
                if row_dict is not None:
                    result_str += f"Row {i+1}: {row_dict}\n"
                else:
                    result_str += f"Row {i+1}: {row}\n"
        
        if len(sql_result) > 10:
            result_str += f"\n... and {len(sql_result) - 10} more rows (total: {len(sql_result)} rows)"
        
        context_instruction = "Explain these query results in clear business language."
    
    prompt = f"""You are a business analyst. Your task is to explain SQL query results to a non-technical user in clear, business language.

## User's Original Question

{question}

## SQL Query Executed

```sql
{sql_query}
```

## Raw Query Results

{result_str}

{context_instruction}

## Answer Format Guidelines

1. **Be Direct**: Start with the answer, not with "Based on the data..."
2. **Use Plain English**: Avoid SQL terminology. Say "total" not "COUNT(*)", "groups" not "GROUP BY"
3. **Include Numbers**: Always mention specific values and counts from the results
4. **Be Conversational**: Write like you're briefing a colleague, not a robot reading data
5. **Keep It Short**: 1-3 sentences for straightforward answers, max 5 for complex findings
6. **No Apologizing**: Don't say "Unfortunately" or "I'm afraid" - just state facts

## Your Answer

Provide a clear, analyst-style summary of these results:"""

    return prompt


def get_result_summary(sql_result: list) -> dict:
    """
    Get metadata about the query result for UI display.
    
    Returns a dictionary with:
    - result_type: 'empty', 'single_number', 'grouped', 'wide', 'list', 'default'
    - readable_description: Human-readable description of the result type
    - num_rows: Number of rows returned
    - num_cols: Number of columns (if applicable)
    - sample_value: Sample value from result (for single_number type)
    
    Args:
        sql_result (list): Raw results from SQL query
        
    Returns:
        dict: Result summary metadata
    """
    result_type, analysis = detect_result_type(sql_result)
    
    descriptions = {
        'empty': '📭 Empty result - no matching records found',
        'single_number': '🎯 Single value - direct answer',
        'single_row': '📋 Single record returned',
        'grouped': f"📊 Grouped data - {analysis.get('rows', 0)} groups",
        'wide': f"📈 Detailed data - {analysis.get('cols', 0)} columns",
        'list': f"📝 List of values - {analysis.get('rows', 0)} items",
        'default': '📄 Query results'
    }
    
    summary = {
        'result_type': result_type,
        'readable_description': descriptions.get(result_type, 'Query results'),
        'num_rows': analysis.get('rows', 0),
        'num_cols': analysis.get('cols', 1),
    }
    
    if result_type == 'single_number':
        summary['sample_value'] = analysis.get('value')
    
    return summary
