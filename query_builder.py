import streamlit as st
import pandas as pd
import json
import time
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from st_aggrid.shared import GridUpdateMode

class QueryBuilder:
    """Provides a drag-and-drop interface for building SQL queries visually."""
    
    def render_query_builder(self, db_connection):
        """Render the visual query builder interface."""
        st.header("Visual Query Builder")
        
        # Check database connection
        if not db_connection:
            st.error("No database connection available. Please connect to a database first.")
            return
        
        # Get available tables
        tables = self._get_tables(db_connection)
        
        if not tables:
            st.warning("No tables found in the database. Please upload data first.")
            return
        
        # Select tables for the query
        st.subheader("1. Select Tables")
        selected_tables = st.multiselect("Choose tables for your query:", tables)
        
        if not selected_tables:
            st.info("Select at least one table to build a query.")
            return
        
        # Get columns for selected tables
        table_columns = {}
        for table in selected_tables:
            columns = self._get_columns(db_connection, table)
            table_columns[table] = columns
        
        # Build query components
        query_components = self._build_query_components(db_connection, selected_tables, table_columns)
        
        # Generate and preview SQL
        generated_sql = self._generate_sql(query_components)
        
        st.subheader("Generated SQL Query")
        st.code(generated_sql, language="sql")
        
        # Execute query options
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("Execute Query"):
                self._execute_query(db_connection, generated_sql)
        
        with col2:
            if st.button("Save Query"):
                query_name = st.text_input("Enter a name for this query:")
                if query_name and st.button("Confirm Save"):
                    self._save_query(query_name, generated_sql)
        
        with col3:
            if st.button("Run in Background"):
                self._run_in_background(generated_sql)
    
    def _get_tables(self, db_connection):
        """Get all tables from the database."""
        from db_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        # Detect database type
        db_type = "mysql"
        if "psycopg2" in str(type(db_connection)):
            db_type = "postgresql"
        elif "sqlite3" in str(type(db_connection)):
            db_type = "sqlite"
            
        # Get tables using database manager
        tables = db_manager.get_all_tables(db_connection, db_type)
        return tables
    
    def _get_columns(self, db_connection, table):
        """Get all columns for a table."""
        from db_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        # Detect database type
        db_type = "mysql"
        if "psycopg2" in str(type(db_connection)):
            db_type = "postgresql"
        elif "sqlite3" in str(type(db_connection)):
            db_type = "sqlite"
            
        # Get columns using database manager
        columns = db_manager.get_table_columns(db_connection, table, db_type)
        return columns
    
    def _build_query_components(self, db_connection, selected_tables, table_columns):
        """Build the components of the query through the visual interface."""
        st.subheader("2. Select Columns")
        
        # Create a flattened list of all columns with their table names
        all_columns = []
        for table in selected_tables:
            for column in table_columns[table]:
                all_columns.append(f"{table}.{column}")
        
        # Select columns for SELECT clause
        select_columns = st.multiselect(
            "Choose columns to include in the result:",
            all_columns,
            default=all_columns[:min(5, len(all_columns))]
        )
        
        # Handle aggregations
        st.subheader("3. Apply Aggregations (Optional)")
        use_aggregations = st.checkbox("Use aggregation functions")
        
        aggregations = {}
        if use_aggregations and select_columns:
            for column in select_columns:
                agg_function = st.selectbox(
                    f"Aggregation for {column}:",
                    ["None", "SUM", "AVG", "COUNT", "MIN", "MAX"],
                    key=f"agg_{column}"
                )
                if agg_function != "None":
                    aggregations[column] = agg_function
        
        # Join conditions if multiple tables selected
        joins = []
        if len(selected_tables) > 1:
            st.subheader("4. Define Joins")
            
            for i in range(len(selected_tables) - 1):
                left_table = selected_tables[i]
                right_table = selected_tables[i + 1]
                
                st.write(f"Join between {left_table} and {right_table}:")
                
                join_type = st.selectbox(
                    "Join type:",
                    ["INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN"],
                    key=f"join_type_{i}"
                )
                
                left_column = st.selectbox(
                    f"Column from {left_table}:",
                    table_columns[left_table],
                    key=f"left_col_{i}"
                )
                
                right_column = st.selectbox(
                    f"Column from {right_table}:",
                    table_columns[right_table],
                    key=f"right_col_{i}"
                )
                
                joins.append({
                    "left_table": left_table,
                    "right_table": right_table,
                    "join_type": join_type,
                    "left_column": left_column,
                    "right_column": right_column
                })
        
        # WHERE conditions
        st.subheader("5. Define Filters (WHERE)")
        
        add_where = st.checkbox("Add WHERE conditions")
        where_conditions = []
        
        if add_where:
            num_conditions = st.number_input("Number of conditions:", min_value=1, max_value=10, value=1)
            
            for i in range(int(num_conditions)):
                st.write(f"Condition {i + 1}:")
                
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    column = st.selectbox(
                        "Column:",
                        all_columns,
                        key=f"where_col_{i}"
                    )
                
                with col2:
                    operator = st.selectbox(
                        "Operator:",
                        ["=", ">", "<", ">=", "<=", "!=", "LIKE", "IN", "BETWEEN"],
                        key=f"where_op_{i}"
                    )
                
                with col3:
                    if operator == "IN":
                        value = st.text_input(
                            "Values (comma separated):",
                            key=f"where_val_{i}"
                        )
                    elif operator == "BETWEEN":
                        val1 = st.text_input("Minimum value:", key=f"where_val_{i}_min")
                        val2 = st.text_input("Maximum value:", key=f"where_val_{i}_max")
                        value = f"{val1} AND {val2}"
                    else:
                        value = st.text_input(
                            "Value:",
                            key=f"where_val_{i}"
                        )
                
                if i > 0:
                    condition_operator = st.radio(
                        "Combine with previous condition using:",
                        ["AND", "OR"],
                        key=f"where_cond_op_{i}"
                    )
                else:
                    condition_operator = None
                
                where_conditions.append({
                    "column": column,
                    "operator": operator,
                    "value": value,
                    "condition_operator": condition_operator
                })
        
        # GROUP BY
        st.subheader("6. Group By (Optional)")
        use_group_by = st.checkbox("Add GROUP BY clause")
        
        group_by_columns = []
        if use_group_by:
            # Only show columns that are not aggregated
            non_aggregated_columns = [col for col in select_columns if col not in aggregations]
            
            if non_aggregated_columns:
                group_by_columns = st.multiselect(
                    "Group by columns:",
                    non_aggregated_columns
                )
        
        # ORDER BY
        st.subheader("7. Order Results (Optional)")
        use_order_by = st.checkbox("Add ORDER BY clause")
        
        order_by = []
        if use_order_by:
            num_order_columns = st.number_input("Number of columns to sort by:", min_value=1, max_value=5, value=1)
            
            for i in range(int(num_order_columns)):
                col1, col2 = st.columns(2)
                
                with col1:
                    column = st.selectbox(
                        "Column:",
                        select_columns,
                        key=f"order_col_{i}"
                    )
                
                with col2:
                    direction = st.selectbox(
                        "Direction:",
                        ["ASC", "DESC"],
                        key=f"order_dir_{i}"
                    )
                
                order_by.append({
                    "column": column,
                    "direction": direction
                })
        
        # LIMIT clause
        st.subheader("8. Limit Results (Optional)")
        use_limit = st.checkbox("Add LIMIT clause")
        limit_value = None
        
        if use_limit:
            limit_value = st.number_input("Maximum number of rows:", min_value=1, value=100)
        
        # Return all query components
        return {
            "tables": selected_tables,
            "select_columns": select_columns,
            "aggregations": aggregations,
            "joins": joins,
            "where_conditions": where_conditions,
            "group_by": group_by_columns,
            "order_by": order_by,
            "limit": limit_value
        }
    
    def _generate_sql(self, query_components):
        """Generate SQL query from the visual builder components."""
        # Detect database type for proper quoting
        import streamlit as st
        db_type = st.session_state.get('current_db', 'postgresql').lower()
        
        # Determine the quote character based on database type
        quote_char = '"' if db_type == 'postgresql' else '`'
        
        # SELECT clause
        select_parts = []
        for column in query_components["select_columns"]:
            if column in query_components["aggregations"]:
                agg_function = query_components["aggregations"][column]
                
                # Handle column with table name (table.column format)
                if '.' in column:
                    table_name, col_name = column.split('.')
                    if db_type == 'postgresql' and any(c.isupper() for c in table_name):
                        # For PostgreSQL with uppercase table names, need to quote properly
                        quoted_column = f"{quote_char}{table_name}{quote_char}.{quote_char}{col_name}{quote_char}"
                        select_parts.append(f"{agg_function}({quoted_column}) AS {col_name}_{agg_function.lower()}")
                    else:
                        select_parts.append(f"{agg_function}({column}) AS {col_name}_{agg_function.lower()}")
                else:
                    select_parts.append(f"{agg_function}({column}) AS {column}_{agg_function.lower()}")
            else:
                # If it's a direct column reference
                if '.' in column:
                    table_name, col_name = column.split('.')
                    if db_type == 'postgresql' and any(c.isupper() for c in table_name):
                        # Quote PostgreSQL uppercase table names
                        select_parts.append(f"{quote_char}{table_name}{quote_char}.{quote_char}{col_name}{quote_char}")
                    else:
                        select_parts.append(column)
                else:
                    select_parts.append(column)
        
        select_clause = "SELECT " + ", ".join(select_parts)
        
        # FROM clause
        first_table = query_components['tables'][0]
        if db_type == 'postgresql' and any(c.isupper() for c in first_table):
            from_clause = f'FROM {quote_char}{first_table}{quote_char}'
        else:
            from_clause = f"FROM {first_table}"
        
        # JOIN clauses
        join_clauses = []
        for join in query_components["joins"]:
            join_clause = f"{join['join_type']} {join['right_table']} ON {join['left_table']}.{join['left_column']} = {join['right_table']}.{join['right_column']}"
            join_clauses.append(join_clause)
        
        # WHERE clause
        where_clause = ""
        if query_components["where_conditions"]:
            where_parts = []
            for i, condition in enumerate(query_components["where_conditions"]):
                # Format the condition based on operator
                if condition["operator"] == "IN":
                    values = condition["value"].split(",")
                    formatted_values = ", ".join([f"'{value.strip()}'" for value in values])
                    condition_str = f"{condition['column']} IN ({formatted_values})"
                elif condition["operator"] == "BETWEEN":
                    values = condition["value"].split(" AND ")
                    if len(values) == 2:
                        condition_str = f"{condition['column']} BETWEEN '{values[0].strip()}' AND '{values[1].strip()}'"
                    else:
                        condition_str = f"{condition['column']} = '{condition['value']}'"
                elif condition["operator"] == "LIKE":
                    condition_str = f"{condition['column']} LIKE '%{condition['value']}%'"
                else:
                    # Handle numeric values without quotes
                    try:
                        float_value = float(condition["value"])
                        condition_str = f"{condition['column']} {condition['operator']} {condition['value']}"
                    except ValueError:
                        condition_str = f"{condition['column']} {condition['operator']} '{condition['value']}'"
                
                # Add the condition operator (AND/OR) if not the first condition
                if i > 0 and condition["condition_operator"]:
                    where_parts.append(f"{condition['condition_operator']} {condition_str}")
                else:
                    where_parts.append(condition_str)
            
            where_clause = "WHERE " + " ".join(where_parts)
        
        # GROUP BY clause
        group_by_clause = ""
        if query_components["group_by"]:
            group_by_clause = "GROUP BY " + ", ".join(query_components["group_by"])
        
        # ORDER BY clause
        order_by_clause = ""
        if query_components["order_by"]:
            order_by_parts = [f"{item['column']} {item['direction']}" for item in query_components["order_by"]]
            order_by_clause = "ORDER BY " + ", ".join(order_by_parts)
        
        # LIMIT clause
        limit_clause = ""
        if query_components["limit"]:
            limit_clause = f"LIMIT {query_components['limit']}"
        
        # Combine all clauses
        sql_parts = [
            select_clause,
            from_clause
        ]
        
        sql_parts.extend(join_clauses)
        
        if where_clause:
            sql_parts.append(where_clause)
        
        if group_by_clause:
            sql_parts.append(group_by_clause)
        
        if order_by_clause:
            sql_parts.append(order_by_clause)
        
        if limit_clause:
            sql_parts.append(limit_clause)
        
        # Return the complete SQL query
        return "\n".join(sql_parts) + ";"
    
    def _execute_query(self, db_connection, query):
        """Execute the generated SQL query."""
        try:
            from query_manager import QueryManager
            query_manager = QueryManager(sqlite3.connect("sql_gui.db"))
            query_manager.execute_and_display_query(db_connection, query)
        except Exception as e:
            st.error(f"Error executing query: {e}")
    
    def _save_query(self, query_name, query):
        """Save the generated query for future use."""
        try:
            from query_manager import QueryManager
            import sqlite3
            
            query_manager = QueryManager(sqlite3.connect("sql_gui.db"))
            query_manager.save_query(query_name, query)
            st.success(f"Query '{query_name}' saved successfully!")
        except Exception as e:
            st.error(f"Error saving query: {e}")
    
    def _run_in_background(self, query):
        """Submit the query for background processing."""
        try:
            # Ask for email notification
            notify_email = st.checkbox("Notify by email when completed", key="bg_email_notify")
            
            email = ""
            if notify_email:
                email = st.text_input("Email address:", key="bg_email")
            
            if st.button("Confirm Run in Background"):
                from background_processor import BackgroundProcessor
                bg_processor = BackgroundProcessor()
                
                task_id = bg_processor.submit_background_query(query, email if notify_email else None)
                
                if task_id:
                    st.success(f"Query submitted for background processing (Task ID: {task_id})")
                else:
                    st.error("Failed to submit query for background processing")
        except Exception as e:
            st.error(f"Error submitting background query: {e}")

import sqlite3
