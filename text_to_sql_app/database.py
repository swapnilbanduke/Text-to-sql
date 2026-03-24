"""
Database connection and management utilities using SQLAlchemy.
"""

import os
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from langchain_community.utilities import SQLDatabase

# Database path
DB_PATH = os.path.join(os.path.dirname(__file__), "data", "app.db")
DATABASE_URL = f"sqlite:///{DB_PATH}"


def quote_identifier(identifier: str) -> str:
    """
    Safely quote a SQLite identifier.
    """
    escaped_identifier = str(identifier).replace('"', '""')
    return f'"{escaped_identifier}"'


def get_engine() -> Engine:
    """
    Create and return SQLAlchemy engine for SQLite database.
    
    Returns:
        Engine: SQLAlchemy engine connected to SQLite database
    """
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False
    )
    return engine


def get_sql_database() -> SQLDatabase:
    """
    Get LangChain SQLDatabase instance for LLM interactions.
    
    Returns:
        SQLDatabase: LangChain wrapper around SQLAlchemy engine
    """
    engine = get_engine()
    db = SQLDatabase(engine=engine)
    return db


def list_tables() -> list:
    """
    List all tables in the database.
    
    Returns:
        list: List of table names
    """
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    return tables


def drop_all_tables():
    """
    Drop all tables from the database.
    WARNING: This will delete all data!
    """
    engine = get_engine()
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    with engine.begin() as conn:
        for table in tables:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {quote_identifier(table)}")
    
    print(f"Dropped {len(tables)} table(s)")


def run_query(sql: str):
    """
    Execute a SQL query and return results.
    
    Args:
        sql (str): SQL query to execute
        
    Returns:
        list: Query results as list of tuples
    """
    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text(sql))
        rows = result.fetchall()
    
    return rows
