import streamlit as st
import mysql.connector
import psycopg2
import sqlite3
import pandas as pd
import time
import os

class DatabaseManager:
    """Handles all database connections and operations."""
    
    def connect_to_mysql_db(self):
        """Establish connection to MySQL database."""
        try:
            conn = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST", "localhost"),
                user=os.getenv("MYSQL_USER", "root"),
                password=os.getenv("MYSQL_PASSWORD", "root"),
                database=os.getenv("MYSQL_DATABASE", "gui"),
                auth_plugin="mysql_native_password",
                connection_timeout=10
            )
            return conn
        except mysql.connector.Error as err:
            st.error(f"Error connecting to MySQL database: {err}")
            return None
    
    def connect_to_postgres_db(self):
        """Establish connection to PostgreSQL database."""
        try:
            db = st.secrets["postgres"]
            #st.write("DB CONFIG:", db)  # <--- Add this line for debugging
            
            conn = psycopg2.connect(
                host=db["host"],
                port=db["port"],
                user=db["user"],
                password=db["password"],
                dbname=db["database"]
            )
            return conn
        except psycopg2.Error as err:
            st.error(f"Error connecting to PostgreSQL database: {err}")
            return None
    
    def connect_to_sqlite_db(self, db_path):
        """Establish connection to SQLite database."""
        try:
            conn = sqlite3.connect(db_path)
            return conn
        except sqlite3.Error as err:
            st.error(f"Error connecting to SQLite database: {err}")
            return None
    
    def get_all_tables(self, conn, db_type="mysql"):
        """Fetch all table names from the connected database."""
        cursor = conn.cursor()
        tables = []
        
        try:
            if db_type.lower() == "mysql":
                cursor.execute("SHOW TABLES")
                tables = [table[0] for table in cursor.fetchall()]
            elif db_type.lower() == "postgresql":
                cursor.execute("""
                    SELECT table_name FROM information_schema.tables 
                    WHERE table_schema = 'public'
                """)
                tables = [table[0] for table in cursor.fetchall()]
            elif db_type.lower() == "sqlite":
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [table[0] for table in cursor.fetchall()]
            
            return tables
        except Exception as e:
            st.error(f"Error fetching tables: {e}")
            return []
        finally:
            cursor.close()
    
    def get_table_columns(self, conn, table_name, db_type="mysql"):
        """Fetch all column names for a given table."""
        cursor = conn.cursor()
        columns = []
        
        try:
            if db_type.lower() == "mysql":
                cursor.execute(f"DESCRIBE `{table_name}`")
                columns = [col[0] for col in cursor.fetchall()]
            elif db_type.lower() == "postgresql":
                cursor.execute(f"""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = '{table_name}'
                """)
                columns = [col[0] for col in cursor.fetchall()]
            elif db_type.lower() == "sqlite":
                cursor.execute(f"PRAGMA table_info('{table_name}')")
                columns = [col[1] for col in cursor.fetchall()]
            
            return columns
        except Exception as e:
            st.error(f"Error fetching columns for table {table_name}: {e}")
            return []
        finally:
            cursor.close()
    
    def execute_query(self, conn, query, params=None, fetch=True):
        """Execute SQL query and return results, with timing metrics."""
        cursor = conn.cursor()
        start_time = time.time()
        result = None
        
        try:
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if fetch:
                result = cursor.fetchall()
                
            execution_time = time.time() - start_time
            st.session_state.current_query_time = execution_time
            
            # For non-SELECT queries, commit changes
            if not query.strip().lower().startswith("select"):
                conn.commit()
                
            # Store this successful query in history
            if st.session_state.authenticated:
                self.add_to_query_history(query, execution_time)
                
            if result and cursor.description:
                column_names = [desc[0] for desc in cursor.description]
                df_results = pd.DataFrame(result, columns=column_names)
                return df_results, execution_time
            
            return result, execution_time
        
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return None, time.time() - start_time
        finally:
            cursor.close()
    
    def analyze_query(self, conn, query, db_type="mysql"):
        """Analyze a query for performance insights."""
        cursor = conn.cursor()
        
        try:
            if db_type.lower() == "mysql":
                explain_query = f"EXPLAIN ANALYZE {query}"
            elif db_type.lower() == "postgresql":
                explain_query = f"EXPLAIN (ANALYZE, BUFFERS) {query}"
            elif db_type.lower() == "sqlite":
                explain_query = f"EXPLAIN QUERY PLAN {query}"
            else:
                return "Query analysis not supported for this database type."
            
            cursor.execute(explain_query)
            analysis_results = cursor.fetchall()
            
            # Format results based on database type
            if db_type.lower() == "mysql":
                analysis_text = "\n".join([str(row) for row in analysis_results])
            elif db_type.lower() == "postgresql":
                analysis_text = "\n".join([row[0] for row in analysis_results])
            elif db_type.lower() == "sqlite":
                analysis_text = "\n".join([str(row) for row in analysis_results])
            
            # Generate optimization suggestions
            suggestions = self.generate_optimization_suggestions(analysis_text, db_type)
            
            return analysis_text, suggestions
        
        except Exception as e:
            st.error(f"Error analyzing query: {e}")
            return str(e), []
        finally:
            cursor.close()
    
    def generate_optimization_suggestions(self, analysis_text, db_type):
        """Generate optimization suggestions based on query analysis."""
        suggestions = []
        
        # Common patterns that indicate potential for optimization
        if "temporary table" in analysis_text.lower():
            suggestions.append("Query uses temporary tables, which can slow performance. Consider indexing relevant columns.")
        
        if "filesort" in analysis_text.lower():
            suggestions.append("Query uses filesort, which can be expensive. Add an index on the columns in the ORDER BY clause.")
        
        if "full table scan" in analysis_text.lower() or "seq scan" in analysis_text.lower():
            suggestions.append("Query performs a full table scan. Consider adding an index on the columns in the WHERE clause.")
        
        if "using where" in analysis_text.lower() and "using index" not in analysis_text.lower():
            suggestions.append("Query filters rows after fetching from storage. Add indexes on columns used in WHERE clauses.")
        
        # Database-specific suggestions
        if db_type.lower() == "mysql":
            if "rows examined" in analysis_text.lower():
                # Extract rows examined/returned ratio if available
                pass
        
        elif db_type.lower() == "postgresql":
            if "buffers" in analysis_text.lower():
                # Extract buffer usage information
                pass
        
        # Generic suggestion if no specific issues found
        if not suggestions:
            suggestions.append("No obvious optimization opportunities identified. The query appears to be well-optimized.")
        
        return suggestions
    
    def store_data_in_db(self, conn, data, table_name, db_type="mysql"):
        """Store the uploaded data into database, avoiding duplicates."""
        cursor = conn.cursor()
        
        try:
            # Clean column names
            cleaned_columns = [col.strip().replace(" ", "_") for col in data.columns if pd.notna(col)]
            
            # Create table with appropriate syntax for the database type
            if db_type.lower() == "mysql":
                columns_with_types = [f"`{col}` TEXT" for col in cleaned_columns]
                create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(columns_with_types)})"
                
                # Clear the table before inserting new data
                clear_table_query = f"DELETE FROM `{table_name}`"
                
                # Insert query with placeholders
                insert_query = f"INSERT INTO `{table_name}` ({', '.join([f'`{col}`' for col in cleaned_columns])}) VALUES ({', '.join(['%s'] * len(cleaned_columns))})"
            
            elif db_type.lower() == "postgresql":
                columns_with_types = [f"\"{col}\" TEXT" for col in cleaned_columns]
                create_table_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(columns_with_types)})"
                
                # Clear the table before inserting new data
                clear_table_query = f"DELETE FROM \"{table_name}\""
                
                # Insert query with placeholders
                columns_quoted = [f"\"{col}\"" for col in cleaned_columns]
                columns_str = ", ".join(columns_quoted)
                placeholders = ", ".join(['%s'] * len(cleaned_columns))
                insert_query = f"INSERT INTO \"{table_name}\" ({columns_str}) VALUES ({placeholders})"
            
            elif db_type.lower() == "sqlite":
                columns_with_types = [f"\"{col}\" TEXT" for col in cleaned_columns]
                create_table_query = f"CREATE TABLE IF NOT EXISTS \"{table_name}\" ({', '.join(columns_with_types)})"
                
                # Clear the table before inserting new data
                clear_table_query = f"DELETE FROM \"{table_name}\""
                
                # Insert query with placeholders
                columns_quoted = [f"\"{col}\"" for col in cleaned_columns]
                columns_str = ", ".join(columns_quoted)
                placeholders = ", ".join(['?' for _ in range(len(cleaned_columns))])
                insert_query = f"INSERT INTO \"{table_name}\" ({columns_str}) VALUES ({placeholders})"
            
            # Execute create table and clear table queries
            cursor.execute(create_table_query)
            cursor.execute(clear_table_query)
            conn.commit()
            
            # Convert data to list of tuples
            data_tuples = [tuple(row) for row in data.itertuples(index=False)]
            
            # Insert data in chunks with progress bar
            chunk_size = 1000
            total_rows = len(data)
            progress_bar = st.progress(0)
            
            for i in range(0, total_rows, chunk_size):
                chunk = data_tuples[i : i + chunk_size]
                cursor.executemany(insert_query, chunk)
                conn.commit()
                progress_bar.progress((i + len(chunk)) / total_rows)
            
            progress_bar.empty()
            st.success(f"Successfully inserted {total_rows} rows into {table_name}.")
            
            # Record this event in notifications
            if st.session_state.authenticated:
                from notification_manager import NotificationManager
                notification = {
                    "type": "data_upload",
                    "message": f"Table {table_name} was created/updated with {total_rows} rows",
                    "timestamp": time.time(),
                    "username": st.session_state.username
                }
                NotificationManager.add_notification(notification)
            
            return True
        
        except Exception as e:
            st.error(f"Error storing data: {e}")
            return False
        finally:
            cursor.close()
    
    def add_to_query_history(self, query, execution_time):
        """Add a query to the user's history."""
        if not st.session_state.authenticated:
            return
        
        history_item = {
            "query": query,
            "execution_time": execution_time,
            "timestamp": time.time(),
            "username": st.session_state.username
        }
        
        st.session_state.query_history.append(history_item)
    
    def reset_database(self, conn):
        """Drop all tables to reset the database."""
        if not conn:
            st.error("No database connection available.")
            return False
        
        cursor = conn.cursor()
        
        try:
            # Determine database type
            if isinstance(conn, mysql.connector.connection.MySQLConnection):
                db_type = "mysql"
            elif isinstance(conn, psycopg2.extensions.connection):
                db_type = "postgresql"
            elif isinstance(conn, sqlite3.Connection):
                db_type = "sqlite"
            else:
                st.error("Unknown database connection type.")
                return False
            
            # Get all tables and drop them
            tables = self.get_all_tables(conn, db_type)
            
            for table in tables:
                if db_type == "mysql":
                    cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
                elif db_type == "postgresql":
                    cursor.execute(f"DROP TABLE IF EXISTS \"{table}\" CASCADE")
                elif db_type == "sqlite":
                    cursor.execute(f"DROP TABLE IF EXISTS \"{table}\"")
            
            conn.commit()
            
            # Record this event in notifications
            if st.session_state.authenticated:
                from notification_manager import NotificationManager
                notification = {
                    "type": "database_reset",
                    "message": "Database was reset (all tables dropped)",
                    "timestamp": time.time(),
                    "username": st.session_state.username
                }
                NotificationManager.add_notification(notification)
            
            return True
        
        except Exception as e:
            st.error(f"Error resetting database: {e}")
            return False
        finally:
            cursor.close()
