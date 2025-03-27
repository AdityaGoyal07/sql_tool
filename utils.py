import streamlit as st
import sqlite3
import os
import time
import hashlib
import pandas as pd
import json
from datetime import datetime, timedelta

def create_sqlite_connection(db_name="sql_gui.db"):
    """Create and return a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(db_name)
        return conn
    except sqlite3.Error as e:
        st.error(f"Error connecting to SQLite database: {e}")
        return None

def format_sql_for_display(sql):
    """Format SQL query for better display in the UI."""
    # Keywords to capitalize
    keywords = [
        "SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING",
        "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN", "OUTER JOIN",
        "ON", "AND", "OR", "NOT", "IN", "BETWEEN", "LIKE", "IS NULL",
        "IS NOT NULL", "AS", "DISTINCT", "COUNT", "SUM", "AVG", "MIN", "MAX",
        "LIMIT", "OFFSET", "UNION", "UNION ALL", "INSERT INTO", "VALUES",
        "UPDATE", "SET", "DELETE FROM", "CREATE TABLE", "ALTER TABLE",
        "DROP TABLE", "INDEX", "VIEW", "PROCEDURE", "FUNCTION",
        "TRIGGER", "CASE", "WHEN", "THEN", "ELSE", "END", "WITH"
    ]
    
    # Replace keywords with uppercase versions
    formatted_sql = sql
    for keyword in keywords:
        # Use word boundaries to avoid replacing parts of words
        pattern = r'\b' + keyword.lower() + r'\b'
        formatted_sql = re.sub(pattern, keyword, formatted_sql, flags=re.IGNORECASE)
    
    return formatted_sql

def generate_session_id():
    """Generate a unique session ID."""
    return hashlib.sha256(str(time.time()).encode()).hexdigest()

def measure_execution_time(func):
    """Decorator to measure the execution time of a function."""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        print(f"Function {func.__name__} executed in {execution_time:.4f} seconds")
        return result, execution_time
    return wrapper

def is_long_running_query(query):
    """Detect if a query is likely to be long-running based on patterns."""
    # Check for patterns that suggest a long-running query
    query = query.lower()
    
    # Full table scan without WHERE or with complex JOIN
    if "select" in query and "from" in query and "where" not in query and "join" in query:
        return True
    
    # Aggregations on large tables
    if any(agg in query for agg in ["count(*)", "sum(", "avg(", "min(", "max("]) and "group by" in query:
        return True
    
    # Complex subqueries
    if query.count("select") > 2:
        return True
    
    return False

def get_table_row_count(conn, table_name):
    """Get the approximate row count of a table."""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
        count = cursor.fetchone()[0]
        cursor.close()
        return count
    except Exception as e:
        print(f"Error getting row count: {e}")
        return None

def get_table_size_info(conn, table_name):
    """Get size information about a table."""
    try:
        cursor = conn.cursor()
        
        # This works for MySQL
        if hasattr(conn, 'cmd_query'):
            cursor.execute(f"""
                SELECT 
                    TABLE_NAME, 
                    TABLE_ROWS, 
                    DATA_LENGTH/1024/1024 AS data_size_mb,
                    INDEX_LENGTH/1024/1024 AS index_size_mb
                FROM 
                    information_schema.TABLES 
                WHERE 
                    TABLE_SCHEMA = DATABASE() AND 
                    TABLE_NAME = '{table_name}'
            """)
            result = cursor.fetchone()
            if result:
                return {
                    "table_name": result[0],
                    "row_count": result[1],
                    "data_size_mb": result[2],
                    "index_size_mb": result[3],
                    "total_size_mb": result[2] + result[3]
                }
        
        # For other databases, just return row count
        row_count = get_table_row_count(conn, table_name)
        return {
            "table_name": table_name,
            "row_count": row_count,
            "data_size_mb": None,
            "index_size_mb": None,
            "total_size_mb": None
        }
    except Exception as e:
        print(f"Error getting table size info: {e}")
        return {
            "table_name": table_name,
            "row_count": None,
            "data_size_mb": None,
            "index_size_mb": None,
            "total_size_mb": None
        }
    finally:
        cursor.close()

def clean_column_name(name):
    """Clean column name for SQL usage."""
    # Remove special characters and spaces
    cleaned = re.sub(r'[^\w]', '_', name)
    # Ensure it doesn't start with a number
    if cleaned and cleaned[0].isdigit():
        cleaned = 'col_' + cleaned
    return cleaned

def parse_error_message(error_msg):
    """Parse database error messages to provide more user-friendly explanations."""
    error_msg = str(error_msg).lower()
    
    if "syntax error" in error_msg:
        return "There's a syntax error in your SQL query. Check for missing keywords, commas, or parentheses."
    
    if "no such table" in error_msg:
        # Extract table name from the error message
        match = re.search(r"no such table: ([^\s]+)", error_msg)
        if match:
            return f"The table '{match.group(1)}' doesn't exist in the database."
        return "The table you're trying to query doesn't exist in the database."
    
    if "no such column" in error_msg:
        # Extract column name from the error message
        match = re.search(r"no such column: ([^\s]+)", error_msg)
        if match:
            return f"The column '{match.group(1)}' doesn't exist in the table."
        return "One of the columns you're trying to query doesn't exist in the table."
    
    if "duplicate column name" in error_msg:
        return "There's a duplicate column name in your query. Use aliases to differentiate them."
    
    if "foreign key constraint fails" in error_msg:
        return "The operation violates a foreign key constraint. Make sure referenced records exist."
    
    if "unique constraint failed" in error_msg:
        return "The operation violates a unique constraint. A record with the same key already exists."
    
    if "near \"" in error_msg:
        # Extract the problematic part of the query
        match = re.search(r'near "([^"]+)"', error_msg)
        if match:
            return f"There's an error near '{match.group(1)}' in your query."
    
    # Default fallback message
    return f"Database error: {error_msg}"

import re
