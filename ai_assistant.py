import streamlit as st
import pandas as pd
import os
import time
import json
import re
import requests
import sqlite3

class AIAssistant:
    """AI-powered SQL query assistant that generates SQL from natural language."""
    
    def __init__(self):
        
        self.setup_assistant_tables()
        # Initialize memory for conversational context
        if 'ai_assistant_memory' not in st.session_state:
            st.session_state.ai_assistant_memory = []
        if 'ai_assistant_context' not in st.session_state:
            st.session_state.ai_assistant_context = ""
    
    def setup_assistant_tables(self):
        """Create tables for storing AI-generated queries and feedback."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            # Create table for AI-generated queries
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ai_generated_queries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                natural_language TEXT NOT NULL,
                generated_sql TEXT NOT NULL,
                feedback INTEGER,  -- 1 for positive, 0 for negative
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                executed BOOLEAN DEFAULT 0,
                context TEXT,  -- Store conversation context
                FOREIGN KEY (username) REFERENCES users(username)
            )
            ''')
            
            # Create table for conversation history
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS ai_conversation_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                session_id TEXT NOT NULL,
                query TEXT NOT NULL,
                response TEXT NOT NULL,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (username) REFERENCES users(username)
            )
            ''')
            
            # Add context column to ai_generated_queries if it doesn't exist
            try:
                cursor.execute("SELECT context FROM ai_generated_queries LIMIT 1")
            except:
                cursor.execute("ALTER TABLE ai_generated_queries ADD COLUMN context TEXT")
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error setting up AI assistant tables: {e}")
    
    def render_ai_interface(self, db_connection):
        """Display the AI-powered SQL assistant interface."""
        st.header("AI Query Assistant")
        
        # Introduction
        st.info("Describe your query in plain English, and I'll generate the SQL for you.")
        
        # Get database schema for context
        schema_info = self._get_database_schema(db_connection)
        
        if not schema_info:
            st.warning("No tables found in the database. Please upload data first.")
            return
        
        # Display database schema for reference
        with st.expander("Database Schema (for reference)"):
            st.json(schema_info)
        
        # Natural language input
        nl_query = st.text_area("Describe what you want to query:", height=100,
                              placeholder="Example: Show me the top 5 products with highest sales")
        
        # Example queries for inspiration
        with st.expander("Example queries you can try"):
            st.write("• Show all customers who made a purchase in the last month")
            st.write("• Find the average sales by product category")
            st.write("• List the top 10 customers by total purchase amount")
            st.write("• Which products have never been ordered?")
            st.write("• Show monthly sales trends for the past year")
        
        # Generate SQL button
        if st.button("Generate SQL") and nl_query:
            with st.spinner("Generating SQL query..."):
                generated_sql = self._generate_sql_from_natural_language(nl_query, schema_info)
                
                if generated_sql:
                    st.session_state.last_generated_sql = generated_sql
                    st.session_state.last_nl_query = nl_query
                    
                    # Display the generated SQL
                    st.subheader("Generated SQL Query")
                    st.code(generated_sql, language="sql")
                    
                    # Save to history
                    self._save_generated_query(nl_query, generated_sql)
                    
                    # Options for using the generated SQL
                    col1, col2, col3 = st.columns(3)
                    
                    with col1:
                        if st.button("Execute Query"):
                            self._execute_query(db_connection, generated_sql)
                            # Mark as executed in history
                            self._mark_query_executed(nl_query, generated_sql)
                    
                    with col2:
                        if st.button("Edit Query"):
                            edited_sql = st.text_area("Edit the SQL:", value=generated_sql, height=200)
                            if st.button("Execute Edited Query"):
                                self._execute_query(db_connection, edited_sql)
                    
                    with col3:
                        if st.button("Save Query"):
                            query_name = st.text_input("Enter a name for this query:")
                            if query_name and st.button("Confirm Save"):
                                self._save_query(query_name, generated_sql)
                    
                    # Feedback buttons
                    st.subheader("Feedback")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button("👍 This is helpful"):
                            self._save_feedback(nl_query, generated_sql, 1)
                            st.success("Thank you for your feedback!")
                    
                    with col2:
                        if st.button("👎 This needs improvement"):
                            self._save_feedback(nl_query, generated_sql, 0)
                            st.success("Thank you for your feedback! We'll improve our AI assistant.")
                else:
                    st.error("Failed to generate SQL. Please try a different query or check your database schema.")
        
        # Show history of generated queries
        st.subheader("Previously Generated Queries")
        previous_queries = self._get_previous_queries()
        
        if previous_queries:
            selected_query_idx = st.selectbox(
                "Select a previous query:",
                range(len(previous_queries)),
                format_func=lambda i: f"{previous_queries[i]['natural_language'][:50]}... ({previous_queries[i]['created_at']})"
            )
            
            selected_query = previous_queries[selected_query_idx]
            
            st.write(f"**Original description:** {selected_query['natural_language']}")
            st.code(selected_query['generated_sql'], language="sql")
            
            if st.button("Use This Query"):
                self._execute_query(db_connection, selected_query['generated_sql'])
        else:
            st.info("You haven't generated any queries yet.")
    
    def _get_database_schema(self, db_connection):
        """Extract database schema information for the AI assistant."""
        schema_info = {}
        
        try:
            # Import DatabaseManager to handle database-specific operations
            from db_manager import DatabaseManager
            db_manager = DatabaseManager()
            
            # Detect database type
            db_type = "mysql"
            if "psycopg2" in str(type(db_connection)):
                db_type = "postgresql"
            elif "sqlite3" in str(type(db_connection)):
                db_type = "sqlite"
            
            # Get all tables using DatabaseManager
            tables = db_manager.get_all_tables(db_connection, db_type)
            
            if not tables:
                return {}
                
            cursor = db_connection.cursor()
            
            # Get schema for each table
            for table in tables:
                # Get column information based on database type
                if db_type == "postgresql":
                    query = f"""
                        SELECT column_name, data_type, 
                               CASE WHEN is_nullable = 'YES' THEN 'YES' ELSE 'NO' END as nullable,
                               CASE WHEN column_default IS NOT NULL THEN column_default ELSE '' END as default_value,
                               CASE WHEN pk.column_name IS NOT NULL THEN 'PRI' ELSE '' END as key_type,
                               '' as extra
                        FROM information_schema.columns c
                        LEFT JOIN (
                            SELECT kcu.column_name
                            FROM information_schema.table_constraints tc
                            JOIN information_schema.key_column_usage kcu
                              ON tc.constraint_name = kcu.constraint_name
                             AND tc.table_name = kcu.table_name
                            WHERE tc.constraint_type = 'PRIMARY KEY'
                              AND tc.table_name = '{table}'
                        ) pk ON c.column_name = pk.column_name
                        WHERE c.table_name = '{table}'
                    """
                    cursor.execute(query)
                    columns = cursor.fetchall()
                    
                    # Format column information
                    schema_info[table] = [
                        {
                            "name": column[0],
                            "type": column[1],
                            "nullable": column[2] == "YES",
                            "key": column[4],
                            "default": column[3],
                            "extra": column[5]
                        }
                        for column in columns
                    ]
                else:
                    # Original MySQL approach
                    cursor.execute(f"DESCRIBE `{table}`")
                    columns = cursor.fetchall()
                    
                    # Format column information
                    schema_info[table] = [
                        {
                            "name": column[0],
                            "type": column[1],
                            "nullable": column[2] == "YES",
                            "key": column[3],
                            "default": column[4],
                            "extra": column[5]
                        }
                        for column in columns
                    ]
                
                # Get a sample of data for better context
                try:
                    if db_type == "postgresql":
                        cursor.execute(f"SELECT * FROM \"{table}\" LIMIT 3")
                    else:
                        cursor.execute(f"SELECT * FROM `{table}` LIMIT 3")
                        
                    sample_rows = cursor.fetchall()
                    
                    if sample_rows:
                        sample_data = []
                        column_names = [col["name"] for col in schema_info[table]]
                        
                        for row in sample_rows:
                            sample_row = {}
                            for i, value in enumerate(row):
                                if i < len(column_names):
                                    sample_row[column_names[i]] = str(value)
                            sample_data.append(sample_row)
                        
                        schema_info[table + "_sample"] = sample_data
                except Exception as e:
                    st.warning(f"Could not fetch sample data for table {table}: {e}")
            
            return schema_info
        except Exception as e:
            st.error(f"Error fetching database schema: {e}")
            return {}
    
    def _generate_sql_from_natural_language(self, nl_query, schema_info):
        """Generate SQL query from natural language using OpenAI API with memory of past queries."""
        try:
            # Use OpenAI API to generate SQL from natural language
            api_key = os.getenv("OPENAI_API_KEY", "")
            
            if not api_key:
                # Simulate AI generation when API key is not available
                return self._simulate_sql_generation(nl_query, schema_info)
            
            # Prepare schema context for the prompt
            schema_context = "Database Schema:\n"
            for table, columns in schema_info.items():
                if not table.endswith("_sample"):
                    schema_context += f"Table: {table}\n"
                    schema_context += "Columns:\n"
                    for column in columns:
                        schema_context += f"- {column['name']} ({column['type']})\n"
                    schema_context += "\n"
            
            # Get conversation history to provide context
            conversation_history = self._get_conversation_history(5)  # Get last 5 exchanges
            conversation_context = ""
            
            if conversation_history:
                conversation_context = "Previous conversation history:\n"
                for entry in conversation_history:
                    conversation_context += f"User: {entry['query']}\n"
                    conversation_context += f"Generated SQL: {entry['response']}\n\n"
            
            # Create the prompt for the API with memory context
            prompt = f"""
            {schema_context}
            
            {conversation_context}
            
            Natural language query: "{nl_query}"
            
            Generate a SQL query that answers this question. The query should be compatible with PostgreSQL syntax.
            Only return the SQL query without any explanation.
            """
            
            # Add this interaction to memory
            if len(st.session_state.ai_assistant_memory) > 10:
                # Keep only the last 10 interactions
                st.session_state.ai_assistant_memory = st.session_state.ai_assistant_memory[-9:]
            
            st.session_state.ai_assistant_memory.append({"query": nl_query, "schema_info": schema_info})
            
            # Update context string
            st.session_state.ai_assistant_context = conversation_context
            
            # Make API request to OpenAI
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}"
            }
            
            data = {
                "model": "gpt-3.5-turbo",
                "messages": [
                    {"role": "system", "content": "You are a SQL expert that converts natural language into SQL queries. Focus on PostgreSQL compliant syntax."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.3
            }
            
            response = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=data
            )
            
            if response.status_code == 200:
                result = response.json()
                generated_sql = result['choices'][0]['message']['content'].strip()
                
                # Clean up the SQL (remove markdown formatting if present)
                if generated_sql.startswith("```sql"):
                    generated_sql = generated_sql.replace("```sql", "").replace("```", "").strip()
                
                return generated_sql
            else:
                st.error(f"API Error: {response.status_code} - {response.text}")
                return self._simulate_sql_generation(nl_query, schema_info)
        
        except Exception as e:
            st.error(f"Error generating SQL: {e}")
            # Fall back to simulated generation
            return self._simulate_sql_generation(nl_query, schema_info)
    
    def _simulate_sql_generation(self, nl_query, schema_info):
        """Simulate SQL generation when API is not available."""
        # Extract table and column names from the schema
        tables = [table for table in schema_info.keys() if not table.endswith("_sample")]
        
        if not tables:
            return "-- No tables available to generate query"
        
        # Simple pattern matching for common query types
        nl_query = nl_query.lower()
        
        # Default to first table if we can't determine a specific one
        main_table = tables[0]
        
        # Try to identify the table from the query
        for table in tables:
            if table.lower() in nl_query:
                main_table = table
                break
        
        # Get columns for the main table
        columns = [col["name"] for col in schema_info.get(main_table, [])]
        
        if not columns:
            columns = ["*"]
        
        # Pattern matching for query types
        if any(word in nl_query for word in ["top", "highest", "most", "best", "largest"]):
            limit = 5
            for num in re.findall(r'\b(\d+)\b', nl_query):
                limit = int(num)
                break
            
            order_direction = "DESC"
            
            # Try to identify a numeric column for ordering
            numeric_col = next((col["name"] for col in schema_info.get(main_table, []) 
                              if col["type"].startswith(("int", "float", "double", "decimal"))), columns[0])
            
            return f"""SELECT * 
FROM {main_table}
ORDER BY {numeric_col} {order_direction}
LIMIT {limit};"""
        
        elif any(word in nl_query for word in ["average", "avg", "mean"]):
            # Find a numeric column for averaging
            numeric_col = next((col["name"] for col in schema_info.get(main_table, []) 
                              if col["type"].startswith(("int", "float", "double", "decimal"))), columns[0])
            
            group_by_col = next((col["name"] for col in schema_info.get(main_table, [])
                               if not col["type"].startswith(("int", "float", "double", "decimal"))), None)
            
            if group_by_col:
                return f"""SELECT {group_by_col}, AVG({numeric_col}) as average_{numeric_col}
FROM {main_table}
GROUP BY {group_by_col}
ORDER BY average_{numeric_col} DESC;"""
            else:
                return f"""SELECT AVG({numeric_col}) as average_{numeric_col}
FROM {main_table};"""
        
        elif any(word in nl_query for word in ["count", "how many"]):
            count_col = "*"
            
            return f"""SELECT COUNT({count_col}) as count
FROM {main_table};"""
        
        elif any(word in nl_query for word in ["recent", "latest", "newest"]):
            # Find a date column
            date_col = next((col["name"] for col in schema_info.get(main_table, []) 
                           if any(date_type in col["type"].lower() for date_type in ["date", "time"])), None)
            
            if date_col:
                return f"""SELECT *
FROM {main_table}
ORDER BY {date_col} DESC
LIMIT 10;"""
        
        # Default to a basic SELECT query
        return f"""SELECT {', '.join(columns[:5])}
FROM {main_table}
LIMIT 10;"""
    
    def _execute_query(self, db_connection, query):
        """Execute the generated SQL query."""
        try:
            from query_manager import QueryManager
            query_manager = QueryManager(sqlite3.connect("sql_gui.db"))
            query_manager.execute_and_display_query(db_connection, query)
        except Exception as e:
            st.error(f"Error executing query: {e}")
    
    def _save_generated_query(self, nl_query, generated_sql):
        """Save an AI-generated query to the history and conversation memory."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            # Get current context
            context = st.session_state.ai_assistant_context
            
            cursor.execute(
                """
                INSERT INTO ai_generated_queries 
                (username, natural_language, generated_sql, context) 
                VALUES (?, ?, ?, ?)
                """,
                (st.session_state.username, nl_query, generated_sql, context)
            )
            
            conn.commit()
            conn.close()
            
            # Also save to conversation history for context
            self._save_conversation_history(nl_query, generated_sql)
            
        except Exception as e:
            print(f"Error saving generated query: {e}")
    
    def _mark_query_executed(self, nl_query, generated_sql):
        """Mark a query as executed in the history."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            cursor.execute(
                """
                UPDATE ai_generated_queries 
                SET executed = 1 
                WHERE username = ? AND natural_language = ? AND generated_sql = ?
                """,
                (st.session_state.username, nl_query, generated_sql)
            )
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error marking query as executed: {e}")
    
    def _save_feedback(self, nl_query, generated_sql, feedback):
        """Save user feedback on an AI-generated query."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            cursor.execute(
                """
                UPDATE ai_generated_queries 
                SET feedback = ? 
                WHERE username = ? AND natural_language = ? AND generated_sql = ?
                """,
                (feedback, st.session_state.username, nl_query, generated_sql)
            )
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error saving feedback: {e}")
    
    def _get_previous_queries(self):
        """Get previously generated queries for the current user."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT id, natural_language, generated_sql, feedback, created_at, executed
                FROM ai_generated_queries
                WHERE username = ?
                ORDER BY created_at DESC
                LIMIT 10
                """,
                (st.session_state.username,)
            )
            
            queries = cursor.fetchall()
            conn.close()
            
            # Convert to list of dictionaries
            return [
                {
                    "id": query[0],
                    "natural_language": query[1],
                    "generated_sql": query[2],
                    "feedback": query[3],
                    "created_at": query[4],
                    "executed": bool(query[5])
                }
                for query in queries
            ]
        except Exception as e:
            print(f"Error getting previous queries: {e}")
            return []
    
    def _save_query(self, query_name, query):
        """Save the generated query for future use."""
        try:
            from query_manager import QueryManager
            query_manager = QueryManager(sqlite3.connect("sql_gui.db"))
            query_manager.save_query(query_name, query)
            st.success(f"Query '{query_name}' saved successfully!")
        except Exception as e:
            st.error(f"Error saving query: {e}")
            
    def _get_conversation_history(self, limit=5):
        """Get recent conversation history to build context for AI.
        
        Args:
            limit: Maximum number of recent interactions to return
            
        Returns:
            List of dictionaries with query/response pairs
        """
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            # First check if we have history in the database
            cursor.execute(
                """
                SELECT query, response
                FROM ai_conversation_history
                WHERE username = ?
                ORDER BY timestamp DESC
                LIMIT ?
                """,
                (st.session_state.username, limit)
            )
            
            history = cursor.fetchall()
            conn.close()
            
            # If we have history in the database, use it
            if history:
                return [
                    {
                        "query": row[0],
                        "response": row[1]
                    }
                    for row in history
                ]
            
            # Otherwise check session state memory
            elif st.session_state.ai_assistant_memory:
                # Convert memory to proper format (just take the last few)
                memory_entries = st.session_state.ai_assistant_memory[-limit:]
                return [
                    {
                        "query": entry["query"],
                        "response": "-- This was a previous query" # Placeholder since we may not have generated SQL yet
                    }
                    for entry in memory_entries
                ]
            
            # No history found in either place
            return []
            
        except Exception as e:
            print(f"Error getting conversation history: {e}")
            return []
            
    def _save_conversation_history(self, query, response):
        """Save a conversation exchange to the database for future context."""
        try:
            # Generate a unique session ID if one doesn't exist
            if 'conversation_session_id' not in st.session_state:
                import uuid
                st.session_state.conversation_session_id = str(uuid.uuid4())
                
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            cursor.execute(
                """
                INSERT INTO ai_conversation_history
                (username, session_id, query, response)
                VALUES (?, ?, ?, ?)
                """,
                (
                    st.session_state.username,
                    st.session_state.conversation_session_id,
                    query,
                    response
                )
            )
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error saving conversation history: {e}")
