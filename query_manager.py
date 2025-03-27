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
        
        # Show available tables with dropdown
        if tables:
            st.subheader("Available Tables")
            
            # Table selection dropdown
            selected_table = st.selectbox(
                "Select a table to view its schema:",
                tables,
                key="schema_table_selector"
            )
            
            # Show schema button
            if st.button("Show Schema", key="show_schema_btn"):
                try:
                    st.subheader(f"Schema for {selected_table}")
                    
                    # Get columns using database-specific methods
                    columns = db_manager.get_table_columns(db_connection, selected_table, db_type)
                    
                    if db_type == "postgresql":
                        # For PostgreSQL, display a simpler schema view
                        query = f"""
                            SELECT column_name, data_type, 
                                   CASE WHEN is_nullable = 'YES' THEN 'YES' ELSE 'NO' END as nullable,
                                   column_default as default_value
                            FROM information_schema.columns 
                            WHERE table_name = '{selected_table}'
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
                        cursor.execute(f"DESCRIBE `{selected_table}`")
                        columns = cursor.fetchall()
                        schema_df = pd.DataFrame(columns, 
                                                columns=["Field", "Type", "Null", "Key", "Default", "Extra"])
                        st.dataframe(schema_df)
                except Exception as e:
                    st.error(f"Error fetching schema: {e}")
                    
            # Display table count for reference
            st.info(f"Total tables available: {len(tables)}")
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
                import re
                
                # First, handle any tables already quoted correctly
                already_quoted_tables = re.findall(r'"([^"]+)"', query)
                
                # Identify all table names from FROM and JOIN clauses
                potential_tables = set()
                from_matches = re.findall(r'FROM\s+([a-zA-Z0-9_\.]+)', query, re.IGNORECASE)
                join_matches = re.findall(r'JOIN\s+([a-zA-Z0-9_\.]+)', query, re.IGNORECASE)
                
                for match in from_matches + join_matches:
                    # Skip already properly quoted tables
                    if match not in already_quoted_tables:
                        potential_tables.add(match)
                
                # Quote all identified table names properly
                for table in potential_tables:
                    # Handle table references without schema
                    query = re.sub(rf'\bFROM\s+{table}\b', f'FROM "{table}"', query, flags=re.IGNORECASE)
                    query = re.sub(rf'\bJOIN\s+{table}\b', f'JOIN "{table}"', query, flags=re.IGNORECASE)
                    
                    # Handle table.column references - need to quote both parts
                    # Replace table.column with "table"."column"
                    query = re.sub(rf'{table}\.([a-zA-Z0-9_]+)', f'"{table}"."\\1"', query)
                
                # Handle column references in various clauses
                # ORDER BY clause
                if 'ORDER BY' in query.upper():
                    # Add quotes to column names in ORDER BY clause when they're not already quoted
                    # and not part of a table.column pattern
                    order_by_parts = re.findall(r'ORDER BY\s+(.*?)(?:LIMIT|$)', query, re.IGNORECASE | re.DOTALL)
                    if order_by_parts:
                        order_items = order_by_parts[0].split(',')
                        for i, item in enumerate(order_items):
                            item = item.strip()
                            # Skip if already quoted or contains a function
                            if (not item.startswith('"') and not re.search(r'\(.*\)', item) and 
                                not re.search(r'\s+AS\s+', item, re.IGNORECASE)):
                                # Handle column name with optional ASC/DESC
                                parts = item.split()
                                col_name = parts[0]
                                # Only quote if it's not a table.column already quoted
                                if '.' not in col_name:
                                    quoted_col = f'"{col_name}"'
                                    if len(parts) > 1:
                                        quoted_col += ' ' + ' '.join(parts[1:])
                                    order_items[i] = quoted_col
                        
                        # Rebuild ORDER BY clause
                        new_order_by = 'ORDER BY ' + ', '.join(order_items)
                        query = re.sub(r'ORDER BY\s+(.*?)(?:LIMIT|$)', new_order_by, query, flags=re.IGNORECASE | re.DOTALL)
                
                # WHERE clause - more careful handling needed
                if 'WHERE' in query.upper():
                    where_parts = re.findall(r'WHERE\s+(.*?)(?:GROUP BY|HAVING|ORDER BY|LIMIT|$)', query, re.IGNORECASE | re.DOTALL)
                    if where_parts:
                        where_clause = where_parts[0]
                        # Only match isolated column names followed by operators
                        # This avoids matching function calls, quoted values, etc.
                        where_clause = re.sub(
                            r'([^".\w])([a-zA-Z0-9_]+)(\s*)(=|>|<|>=|<=|<>|!=|LIKE|IN|BETWEEN)',
                            r'\1"\2"\3\4',
                            where_clause
                        )
                        # Replace in original query
                        query = query.replace(where_parts[0], where_clause)
                
                # GROUP BY clause
                if 'GROUP BY' in query.upper():
                    group_by_parts = re.findall(r'GROUP BY\s+(.*?)(?:HAVING|ORDER BY|LIMIT|$)', query, re.IGNORECASE | re.DOTALL)
                    if group_by_parts:
                        group_items = group_by_parts[0].split(',')
                        for i, item in enumerate(group_items):
                            item = item.strip()
                            # Skip if already quoted or contains a function
                            if not item.startswith('"') and not re.search(r'\(.*\)', item):
                                # Only quote if it's not a table.column already quoted
                                if '.' not in item:
                                    group_items[i] = f'"{item}"'
                        
                        # Rebuild GROUP BY clause
                        new_group_by = 'GROUP BY ' + ', '.join(group_items)
                        query = re.sub(r'GROUP BY\s+(.*?)(?:HAVING|ORDER BY|LIMIT|$)', new_group_by, query, flags=re.IGNORECASE | re.DOTALL)
                        
                # SELECT clause - quote column names but be careful with aliases and functions
                if query.upper().startswith('SELECT'):
                    select_parts = re.findall(r'SELECT\s+(.*?)\s+FROM', query, re.IGNORECASE | re.DOTALL)
                    if select_parts:
                        select_items = select_parts[0].split(',')
                        for i, item in enumerate(select_items):
                            item = item.strip()
                            # Skip if already quoted, contains functions, has alias, or is a wildcard
                            if (not item.startswith('"') and not re.search(r'\(.*\)', item) and 
                                not re.search(r'\s+AS\s+', item, re.IGNORECASE) and item != '*'):
                                # Only quote if it's not a table.column already quoted
                                if '.' not in item:
                                    select_items[i] = f'"{item}"'
                        
                        # Rebuild SELECT clause
                        new_select = 'SELECT ' + ', '.join(select_items)
                        query = re.sub(r'SELECT\s+(.*?)\s+FROM', new_select + ' FROM', query, flags=re.IGNORECASE | re.DOTALL)
            
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
            # Detect database type
            db_type = "mysql"
            if "psycopg2" in str(type(db_connection)):
                db_type = "postgresql"
            elif "sqlite3" in str(type(db_connection)):
                db_type = "sqlite"
                
            # Apply the correct EXPLAIN syntax based on database type
            if db_type == "mysql":
                explain_query = f"EXPLAIN ANALYZE {query}"
            elif db_type == "postgresql":
                # PostgreSQL uses a different EXPLAIN syntax
                explain_query = f"EXPLAIN (ANALYZE, COSTS, VERBOSE, BUFFERS, FORMAT JSON) {query}"
            else:  # SQLite or others
                explain_query = f"EXPLAIN QUERY PLAN {query}"
            
            cursor.execute(explain_query)
            results = cursor.fetchall()
            
            # Format results based on database type
            if db_type == "mysql":
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
                    
            elif db_type == "postgresql":
                # PostgreSQL returns JSON format for EXPLAIN ANALYZE
                import json
                
                # Parse the JSON result
                try:
                    explain_json = results[0][0]
                    
                    # Display the JSON in a more readable format
                    st.json(explain_json)
                    
                    # Extract useful information from the JSON
                    plan_info = explain_json[0]["Plan"]
                    
                    # Create a summary of the query plan
                    st.subheader("Query Plan Summary")
                    
                    # Basic plan info
                    st.write(f"**Node Type:** {plan_info.get('Node Type', 'N/A')}")
                    st.write(f"**Total Cost:** {plan_info.get('Total Cost', 'N/A')}")
                    st.write(f"**Planning Time:** {explain_json[0].get('Planning Time', 'N/A')} ms")
                    st.write(f"**Execution Time:** {explain_json[0].get('Execution Time', 'N/A')} ms")
                    
                    # Check for sequential scans (full table scans)
                    if plan_info.get('Node Type') == 'Seq Scan':
                        st.warning("⚠️ Sequential Scan detected. Consider adding an index on the search columns.")
                    
                    # Check for index usage
                    if 'Index' in plan_info.get('Node Type', ''):
                        st.success(f"✅ Query is using index: {plan_info.get('Index Name', 'unknown')}")
                    
                    # Warn about high cost operations
                    if plan_info.get('Total Cost', 0) > 1000:
                        st.warning("⚠️ High cost query detected. Consider optimizing.")
                    
                except (json.JSONDecodeError, IndexError, KeyError) as e:
                    st.error(f"Error parsing PostgreSQL EXPLAIN result: {e}")
                    st.dataframe(pd.DataFrame(results))
                
            else:
                # SQLite or other databases
                try:
                    explain_df = pd.DataFrame(results, columns=["id", "detail", "table", "action"])
                except:
                    # Fallback if column names don't match
                    explain_df = pd.DataFrame(results)
                    
                st.dataframe(explain_df)
                
                # Basic analysis for SQLite
                explain_str = str(explain_df)
                if "SCAN TABLE" in explain_str:
                    st.warning("⚠️ Table scan detected. Consider adding an index.")
                if "SEARCH TABLE" in explain_str and "USING INDEX" in explain_str:
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
        st.header("Query History & Saved Queries")
        
        # Create tabs for query history and saved queries
        history_tab, saved_tab = st.tabs(["Query History", "Saved Queries"])
        
        with history_tab:
            try:
                # Load query history from database
                self.cursor.execute(
                    """
                    SELECT qh.query, qh.execution_time, qh.timestamp, 
                           COALESCE(sq.query_name, '') as query_name
                    FROM query_history qh
                    LEFT JOIN saved_queries sq ON qh.query = sq.query AND qh.username = sq.username
                    WHERE qh.username = ? 
                    ORDER BY qh.timestamp DESC LIMIT 100
                    """,
                    (st.session_state.username,)
                )
                history = self.cursor.fetchall()
                
                if not history:
                    st.info("No query history found.")
                    return
                
                # Display history in a table with query names (if saved)
                history_df = pd.DataFrame(history, columns=["Query", "Execution Time (s)", "Timestamp", "Saved As"])
                
                # Highlight saved queries
                styled_df = history_df.style.apply(
                    lambda x: ['background-color: #e6f7ff' if x["Saved As"] else '' for i in range(len(x))], 
                    axis=1
                )
                
                st.dataframe(history_df)
                
                # Allow rerunning queries from history
                st.subheader("Rerun Query from History")
                if history:
                    selected_idx = st.selectbox(
                        "Select a query to rerun:",
                        range(len(history)),
                        format_func=lambda i: (
                            f"[{history[i][3]}] {history[i][0][:50]}... ({history[i][2]})" 
                            if history[i][3] else 
                            f"{history[i][0][:50]}... ({history[i][2]})"
                        )
                    )
                    
                    selected_query = history[selected_idx][0]
                    
                    st.code(selected_query, language="sql")
                    
                    # Add button to run the selected query
                    if st.button("Run Historical Query"):
                        try:
                            # Get current database connection
                            from db_manager import DatabaseManager
                            db_manager = DatabaseManager()
                            db_connection = db_manager.connect_to_mysql_db()  # Default to MySQL
                            
                            if db_connection:
                                self.execute_and_display_query(db_connection, selected_query)
                            else:
                                st.error("Could not connect to database.")
                        except Exception as e:
                            st.error(f"Error executing query: {e}")
                else:
                    st.info("No history queries to run.")
                
            except Exception as e:
                st.error(f"Error loading query history: {e}")
        
        with saved_tab:
            # Display saved queries
            try:
                saved_queries = self.load_saved_queries()
                
                if saved_queries:
                    st.subheader("Your Saved Queries")
                    
                    for i, query in enumerate(saved_queries):
                        with st.expander(f"{query['query_name']} - {query['created_at']}"):
                            # Show query details
                            if query['description']:
                                st.write(f"**Description:** {query['description']}")
                            
                            if query['category']:
                                st.write(f"**Category:** {query['category']}")
                                
                            st.code(query['query'], language="sql")
                            
                            # Provide options to use the query
                            cols = st.columns(2)
                            with cols[0]:
                                if st.button("Use This Query", key=f"use_saved_{i}"):
                                    st.session_state.selected_saved_query = query['query']
                                    st.info("Query loaded in the SQL Query Interface")
                            
                            with cols[1]:
                                if st.button("Delete", key=f"delete_saved_{i}"):
                                    # Add delete functionality here
                                    pass
                else:
                    st.info("No saved queries found.")
            except Exception as e:
                st.error(f"Error loading saved queries: {e}")
            
            # Choose a saved query to run
            saved_queries = self.load_saved_queries()
            if saved_queries:
                selected_saved_idx = st.selectbox(
                    "Select a saved query to run:",
                    range(len(saved_queries)),
                    format_func=lambda i: f"{saved_queries[i]['query_name']}"
                )
                
                selected_saved_query = saved_queries[selected_saved_idx]['query']
                st.code(selected_saved_query, language="sql")
                
                # Add button to rerun selected query
                if st.button("Run Saved Query"):
                    try:
                        # Get current database connection
                        from db_manager import DatabaseManager
                        db_manager = DatabaseManager()
                        db_connection = db_manager.connect_to_mysql_db()  # Default to MySQL
                        
                        if db_connection:
                            self.execute_and_display_query(db_connection, selected_saved_query)
                        else:
                            st.error("Could not connect to database.")
                    except Exception as e:
                        st.error(f"Error executing query: {e}")
            else:
                st.info("No saved queries to run.")
