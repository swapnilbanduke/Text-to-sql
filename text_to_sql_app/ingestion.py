"""
Data ingestion utilities for loading data from CSV/Excel into the database.
"""

import re
import pandas as pd
from io import BytesIO
from sqlalchemy import inspect
from database import get_engine


# SQL reserved words to avoid
SQL_RESERVED_WORDS = {
    'select', 'from', 'where', 'join', 'inner', 'left', 'right', 'outer',
    'on', 'and', 'or', 'not', 'in', 'is', 'null', 'like', 'between',
    'order', 'by', 'group', 'having', 'limit', 'offset', 'distinct',
    'insert', 'update', 'delete', 'create', 'drop', 'alter', 'table',
    'database', 'index', 'view', 'trigger', 'procedure', 'function',
    'default', 'check', 'primary', 'key', 'foreign', 'unique', 'constraint',
    'column', 'schema', 'cascade', 'restrict', 'set', 'no', 'action'
}


def clean_name(name: str) -> str:
    """
    Clean table/column names for SQL compatibility.
    
    Rules:
    - Convert to lowercase
    - Replace spaces with underscores
    - Remove special characters
    - Avoid SQL reserved words by appending underscore
    
    Args:
        name (str): Original name to clean
        
    Returns:
        str: Cleaned name safe for SQL
    """
    # Convert to lowercase
    cleaned = str(name).lower().strip()
    
    # Replace spaces with underscores
    cleaned = re.sub(r'\s+', '_', cleaned)
    
    # Remove special characters, keep only alphanumeric and underscores
    cleaned = re.sub(r'[^a-z0-9_]', '', cleaned)
    
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    
    # Handle empty strings
    if not cleaned:
        cleaned = 'column'
    
    # Avoid SQL reserved words
    if cleaned.lower() in SQL_RESERVED_WORDS:
        cleaned = cleaned + '_'
    
    return cleaned


def read_csv(file_path_or_bytes) -> tuple:
    """
    Read CSV file and return dataframe with name mapping.
    
    Args:
        file_path_or_bytes: Path to CSV file or BytesIO object
        
    Returns:
        tuple: (dataframe, name_mapping_dict)
    """
    try:
        if isinstance(file_path_or_bytes, (str, bytes)):
            if isinstance(file_path_or_bytes, bytes):
                df = pd.read_csv(BytesIO(file_path_or_bytes))
            else:
                df = pd.read_csv(file_path_or_bytes)
        else:
            df = pd.read_csv(file_path_or_bytes)
        
        # Create column name mapping
        name_mapping = {}
        new_columns = {}
        
        for col in df.columns:
            cleaned_col = clean_name(col)
            name_mapping[col] = cleaned_col
            new_columns[col] = cleaned_col
        
        # Rename columns in dataframe
        df = df.rename(columns=new_columns)
        
        return df, name_mapping
    
    except Exception as e:
        print(f"Error reading CSV: {e}")
        raise


def read_excel(file_path_or_bytes) -> dict:
    """
    Read Excel file with multiple sheets.
    
    Each sheet becomes one table in the database.
    
    Args:
        file_path_or_bytes: Path to Excel file or BytesIO object
        
    Returns:
        dict: {table_name: (dataframe, column_mapping)} for each sheet
    """
    try:
        if isinstance(file_path_or_bytes, bytes):
            excel_file = pd.ExcelFile(BytesIO(file_path_or_bytes))
        else:
            excel_file = pd.ExcelFile(file_path_or_bytes)
        
        sheets_data = {}
        
        # Loop through all sheets
        for sheet_name in excel_file.sheet_names:
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            # Clean table name
            cleaned_table_name = clean_name(sheet_name)
            
            # Create column name mapping
            name_mapping = {}
            new_columns = {}
            
            for col in df.columns:
                cleaned_col = clean_name(col)
                name_mapping[col] = cleaned_col
                new_columns[col] = cleaned_col
            
            # Rename columns in dataframe
            df = df.rename(columns=new_columns)
            
            sheets_data[cleaned_table_name] = {
                'dataframe': df,
                'column_mapping': name_mapping,
                'original_sheet_name': sheet_name
            }
        
        return sheets_data
    
    except Exception as e:
        print(f"Error reading Excel: {e}")
        raise


def save_to_sql(df: pd.DataFrame, table_name: str, if_exists: str = "replace") -> bool:
    """
    Save dataframe to SQL database.
    
    Args:
        df (pd.DataFrame): Dataframe to save
        table_name (str): Target table name
        if_exists (str): How to behave if table exists ('fail', 'replace', 'append')
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        engine = get_engine()
        df.to_sql(table_name, con=engine, if_exists=if_exists, index=False)
        print(f"Successfully saved table '{table_name}' with {len(df)} rows")
        return True
    
    except Exception as e:
        print(f"Error saving to SQL: {e}")
        return False


def ingest_csv(file_path_or_bytes, table_name: str = None) -> dict:
    """
    Complete pipeline: ingest CSV file into database.
    
    Args:
        file_path_or_bytes: Path to CSV file or BytesIO object
        table_name (str, optional): Custom table name. If None, uses 'csv_data'
        
    Returns:
        dict: Ingestion report with metadata
    """
    if table_name is None:
        table_name = "csv_data"
    
    table_name = clean_name(table_name)
    
    df, column_mapping = read_csv(file_path_or_bytes)
    
    success = save_to_sql(df, table_name, if_exists="replace")
    
    return {
        'success': success,
        'table_name': table_name,
        'rows': len(df),
        'columns': list(df.columns),
        'column_mapping': column_mapping
    }


def ingest_excel(file_path_or_bytes) -> dict:
    """
    Complete pipeline: ingest Excel file(s) into database.
    
    Each sheet becomes a separate table.
    
    Args:
        file_path_or_bytes: Path to Excel file or BytesIO object
        
    Returns:
        dict: Ingestion report with metadata for all sheets
    """
    sheets_data = read_excel(file_path_or_bytes)
    
    report = {
        'success': True,
        'tables': []
    }
    
    for table_name, sheet_info in sheets_data.items():
        df = sheet_info['dataframe']
        original_name = sheet_info['original_sheet_name']
        column_mapping = sheet_info['column_mapping']
        
        success = save_to_sql(df, table_name, if_exists="replace")
        
        report['tables'].append({
            'original_sheet_name': original_name,
            'sql_table_name': table_name,
            'rows': len(df),
            'columns': list(df.columns),
            'column_mapping': column_mapping,
            'success': success
        })
    
    return report
