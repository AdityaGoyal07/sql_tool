import streamlit as st
import pandas as pd
import time
import json
from datetime import datetime
import sqlite3

class QueryManager:
    """Manages SQL queries, history, and saved queries."""
    
    def __init__(self, sqlite_conn):
        self.conn = sqlite_conn
        self.cursor = self.conn.cursor()
        self.setup_query_tables()
    
    def setup_query_tables(self):
        """Create tables for storing query history and saved queries."""
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS query_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            query TEXT NOT NULL,
            execution_time REAL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS saved_queries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            query_name TEXT NOT NULL,
            query TEXT NOT NULL,
            description TEXT,
            category TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.conn.commit()
    
    def render_query_interface(self, db_connection):
        """Display the SQL query interface."""
        st.header("SQL Query Interface")
        
        # Get available tables using DatabaseManager
        from db_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        # Detect database type
        db_type = "mysql"
        if "psycopg2" in str(type(db_connection)):
            db_type = "postgresql"
        elif "sqlite3" in str(type(db_connection)):
            db_type = "sqlite"
            
        # Get tables using the appropriate method
        tables = db_manager.get_all_tables(db_connection, db_type)
        
        # Show available tables
        if tables:
            st.subheader("Available Tables")
            cols = st.columns(3)
            for i, table in enumerate(tables):
                with cols[i % 3]:
                    st.info(table)
                    if st.button(f"Show Schema", key=f"schema_{table}"):
                        try:
                            # Get columns using database-specific methods
                            columns = db_manager.get_table_columns(db_connection, table, db_type)
                            
                            if db_type == "postgresql":
                                # For PostgreSQL, display a simpler schema view
                                query = f"""
                                    SELECT column_name, data_type, 
                                           CASE WHEN is_nullable = 'YES' THEN 'YES' ELSE 'NO' END as nullable,
                                           column_default as default_value
                                    FROM information_schema.columns 
                                    WHERE table_name = '{table}'
                                """
                                cursor = db_connection.cursor()
                                cursor.execute(query)
                                columns_data = cursor.fetchall()
                                schema_df = pd.DataFrame(columns_data, 
                                                        columns=["Column", "Type", "Nullable", "Default"])
                                st.dataframe(schema_df)
                            else:
                                # For MySQL, use original code
                                cursor = db_connection.cursor()
                                cursor.execute(f"DESCRIBE `{table}`")
                                columns = cursor.fetchall()
                                schema_df = pd.DataFrame(columns, 
                                                        columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
                                st.dataframe(schema_df)
                        except Exception as e:
                            st.error(f"Error fetching schema: {e}")
        else:
            st.warning("No tables found in the database. Please upload data first.")
        
        # Query input
        st.subheader("Write SQL Query")
        
        # Show saved queries dropdown
        saved_queries = self.load_saved_queries()
        if saved_queries:
            selected_query = st.selectbox(
                "Select a saved query or write a new one:",
                ["New Query"] + [f"{q['query_name']}: {q['query'][:50]}..." for q in saved_queries]
            )
            
            if selected_query != "New Query":
                # Extract the query from the selected saved query
                query_idx = [i for i, q in enumerate(saved_queries) 
                            if selected_query.startswith(f"{q['query_name']}:")][0]
                query = saved_queries[query_idx]["query"]
            else:
                query = ""
        else:
            query = ""
        
        query = st.text_area("SQL Query:", value=query, height=150)
        
        # Query execution and options
        cols = st.columns([1, 1, 1])
        with cols[0]:
            execute_button = st.button("Execute Query")
        with cols[1]:
            save_query = st.button("Save Query")
        with cols[2]:
            analyze_query = st.button("Analyze Performance")
        
        # Save query dialog
        if save_query and query:
            st.subheader("Save Query")
            query_name = st.text_input("Query Name:")
            query_desc = st.text_area("Description (optional):")
            query_category = st.text_input("Category (optional):")
            
            if st.button("Confirm Save"):
                self.save_query(query_name, query, query_desc, query_category)
                st.success(f"Query '{query_name}' saved successfully!")
        
        # Execute query
        if execute_button and query:
            self.execute_and_display_query(db_connection, query)
        
        # Analyze query performance
        if analyze_query and query:
            self.analyze_query_performance(db_connection, query)
    
    def execute_and_display_query(self, db_connection, query):
        """Execute a SQL query and display results with performance metrics."""
        cursor = db_connection.cursor()
        
        try:
            # Record start time
            start_time = time.time()
            
            # Determine database type
            db_type = "mysql"
            if "psycopg2" in str(type(db_connection)):
                db_type = "postgresql"
            elif "sqlite3" in str(type(db_connection)):
                db_type = "sqlite"
                
            # For PostgreSQL tables with capital letters or special characters, use double quotes
            if db_type == "postgresql":
                # Replace simple table references with quoted versions for PostgreSQL
                # This regex looks for table names that aren't already in quotes
                import re
                query = re.sub(r'FROM\s+Life_Expectancy_Data\b', 'FROM "Life_Expectancy_Data"', query, flags=re.IGNORECASE)
                query = re.sub(r'JOIN\s+Life_Expectancy_Data\b', 'JOIN "Life_Expectancy_Data"', query, flags=re.IGNORECASE)
                # Handle table.column references
                query = re.sub(r'Life_Expectancy_Data\.([A-Za-z0-9_]+)', r'"Life_Expectancy_Data"."\\1"', query)
                
                # Handle explicit column names in ORDER BY clauses that aren't in table.column format
                if 'ORDER BY' in query:
                    # If we have already quoted column names, don't quote them again
                    if not re.search(r'ORDER BY\s+"', query):
                        query = re.sub(r'ORDER BY\s+([A-Za-z0-9_]+)', r'ORDER BY "\1"', query)
                        
                # Also quote column names in WHERE clauses for non-table prefixed columns
                if 'WHERE' in query:
                    # Match column name followed by an operator
                    query = re.sub(r'WHERE\s+([A-Za-z0-9_]+)\s*(=|>|<|>=|<=|<>|!=|LIKE|IN|BETWEEN)', r'WHERE "\1" \2', query, flags=re.IGNORECASE)
                    
                # Quote columns in GROUP BY clauses
                if 'GROUP BY' in query:
                    query = re.sub(r'GROUP BY\s+([A-Za-z0-9_]+)', r'GROUP BY "\1"', query, flags=re.IGNORECASE)
            
            # Execute query
            cursor.execute(query)
            
            # Get results
            results = cursor.fetchall()
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Format and display results
            if results:
                column_names = [desc[0] for desc in cursor.description]
                df_results = pd.DataFrame(results, columns=column_names)
                
                # Convert object type columns to string to avoid display issues
                for col in df_results.columns:
                    if df_results[col].dtype == "object":
                        df_results[col] = df_results[col].astype(str)
                
                # Store in session state for visualization
                st.session_state.last_query_result = df_results
                
                # Display execution time
                st.success(f"Query executed in {execution_time:.4f} seconds")
                
                # Display results
                st.subheader("Query Results")
                st.dataframe(df_results)
                
                # Show row count
                st.info(f"Returned {len(df_results)} rows")
                
                # Offer visualization options
                st.subheader("Visualize Results")
                if st.button("Create Visualization"):
                    from visualization_manager import VisualizationManager
                    viz_manager = VisualizationManager()
                    viz_manager.render_visualization_options(df_results)
                
                # Save result as CSV
                csv = df_results.to_csv(index=False)
                st.download_button(
                    "Download results as CSV",
                    csv,
                    f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv",
                    key='download-csv'
                )
            else:
                # For non-SELECT queries, show success message
                if not query.strip().lower().startswith("select"):
                    st.success(f"Query executed successfully in {execution_time:.4f} seconds. No results to display.")
                    # Commit changes for non-SELECT queries
                    db_connection.commit()
                else:
                    st.info(f"Query executed in {execution_time:.4f} seconds. No results returned.")
            
            # Store in query history
            self.add_to_query_history(query, execution_time)
            
        except Exception as e:
            st.error(f"Error executing query: {e}")
    
    def analyze_query_performance(self, db_connection, query):
        """Analyze SQL query performance and suggest optimizations."""
        if not query.strip().lower().startswith("select"):
            st.warning("Performance analysis is only available for SELECT queries.")
            return
        
        cursor = db_connection.cursor()
        
        try:
            st.subheader("Query Performance Analysis")
            
            # Execute EXPLAIN ANALYZE
            st.write("Executing EXPLAIN ANALYZE...")
            
            # Determine database type and format EXPLAIN accordingly
            if hasattr(db_connection, 'cmd_query'):  # MySQL
                explain_query = f"EXPLAIN ANALYZE {query}"
            else:  # SQLite or others
                explain_query = f"EXPLAIN QUERY PLAN {query}"
            
            cursor.execute(explain_query)
            results = cursor.fetchall()
            
            # Format results
            if hasattr(db_connection, 'cmd_query'):  # MySQL
                explain_df = pd.DataFrame(results)
                st.dataframe(explain_df)
                
                # Extract key information
                table_scans = explain_df[explain_df[0].astype(str).str.contains("table scan", case=False)]
                if not table_scans.empty:
                    st.warning("⚠️ Full table scan detected. Consider adding an index on the search columns.")
                
                # Index usage analysis
                index_usage = explain_df[explain_df[0].astype(str).str.contains("index", case=False)]
                if index_usage.empty:
                    st.warning("⚠️ No indexes used in this query. Consider adding appropriate indexes.")
                else:
                    st.success("✅ Query is using indexes.")
            else:
                # SQLite or other databases
                explain_df = pd.DataFrame(results, columns=["id", "detail", "table", "action"])
                st.dataframe(explain_df)
                
                # Basic analysis for SQLite
                if "SCAN TABLE" in str(explain_df):
                    st.warning("⚠️ Table scan detected. Consider adding an index.")
                if "SEARCH TABLE" in str(explain_df) and "USING INDEX" in str(explain_df):
                    st.success("✅ Query is using indexes.")
            
            # General recommendations
            st.subheader("Optimization Recommendations")
            
            # Check if query has WHERE clause
            if "where" not in query.lower():
                st.warning("⚠️ Query does not have a WHERE clause. This might return a large result set.")
            
            # Check if query has ORDER BY or GROUP BY without an index
            if "order by" in query.lower() or "group by" in query.lower():
                st.info("ℹ️ Query includes sorting/grouping. Ensure the columns have appropriate indexes.")
            
            # Check for joins without indexes
            if "join" in query.lower():
                st.info("ℹ️ Query includes joins. Ensure join columns have indexes on both tables.")
            
            # Check for LIMIT clause
            if "limit" not in query.lower():
                st.info("ℹ️ Consider adding a LIMIT clause to restrict the result set size.")
            
        except Exception as e:
            st.error(f"Error analyzing query: {e}")
    
    def add_to_query_history(self, query, execution_time):
        """Add a query to the history database."""
        if not st.session_state.authenticated:
            return
        
        try:
            self.cursor.execute(
                "INSERT INTO query_history (username, query, execution_time) VALUES (?, ?, ?)",
                (st.session_state.username, query, execution_time)
            )
            self.conn.commit()
            
            # Also update session state
            if 'query_history' not in st.session_state:
                st.session_state.query_history = []
            
            st.session_state.query_history.append({
                "query": query,
                "execution_time": execution_time,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "username": st.session_state.username
            })
            
        except Exception as e:
            st.error(f"Error storing query history: {e}")
    
    def save_query(self, query_name, query, description="", category=""):
        """Save a query for future use."""
        if not st.session_state.authenticated:
            return
        
        try:
            self.cursor.execute(
                "INSERT INTO saved_queries (username, query_name, query, description, category) VALUES (?, ?, ?, ?, ?)",
                (st.session_state.username, query_name, query, description, category)
            )
            self.conn.commit()
            
            # Update session state
            if 'saved_queries' not in st.session_state:
                st.session_state.saved_queries = []
            
            st.session_state.saved_queries.append({
                "query_name": query_name,
                "query": query,
                "description": description,
                "category": category,
                "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            
        except Exception as e:
            st.error(f"Error saving query: {e}")
    
    def load_saved_queries(self):
        """Load saved queries for the current user."""
        if not st.session_state.authenticated:
            return []
        
        try:
            self.cursor.execute(
                "SELECT query_name, query, description, category, created_at FROM saved_queries WHERE username = ? ORDER BY created_at DESC",
                (st.session_state.username,)
            )
            saved_queries = self.cursor.fetchall()
            
            # Format as list of dictionaries
            return [
                {
                    "query_name": row[0],
                    "query": row[1],
                    "description": row[2],
                    "category": row[3],
                    "created_at": row[4]
                }
                for row in saved_queries
            ]
            
        except Exception as e:
            st.error(f"Error loading saved queries: {e}")
            return []
    
    def render_history_interface(self):
        """Display the query history interface."""
        st.header("Query History")
        
        try:
            # Load query history from database
            self.cursor.execute(
                "SELECT query, execution_time, timestamp FROM query_history WHERE username = ? ORDER BY timestamp DESC LIMIT 100",
                (st.session_state.username,)
            )
            history = self.cursor.fetchall()
            
            if not history:
                st.info("No query history found.")
                return
            
            # Display history in a table
            history_df = pd.DataFrame(history, columns=["Query", "Execution Time (s)", "Timestamp"])
            st.dataframe(history_df)
            
            # Allow rerunning queries from history
            st.subheader("Rerun Query from History")
            selected_idx = st.selectbox(
                "Select a query to rerun:",
                range(len(history)),
                format_func=lambda i: f"{history[i][0][:50]}... ({history[i][2]})"
            )
            
            selected_query = history[selected_idx][0]
            
            st.code(selected_query, language="sql")
            
            if st.button("Rerun Selected Query"):
                # Get current database connection
                from db_manager import DatabaseManager
                db_manager = DatabaseManager()
                db_connection = db_manager.connect_to_mysql_db()  # Default to MySQL
                
                if db_connection:
                    self.execute_and_display_query(db_connection, selected_query)
                else:
                    st.error("Could not connect to database.")
            
        except Exception as e:
            st.error(f"Error loading query history: {e}")
