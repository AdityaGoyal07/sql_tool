import streamlit as st
import time
import threading
import sqlite3
import pandas as pd
from datetime import datetime
import os
from email_service import EmailService

class BackgroundProcessor:
    """Handles background processing of long-running queries and notifies users upon completion."""
    
    def __init__(self):
        self.setup_background_tables()
        self.email_service = EmailService()
    
    def setup_background_tables(self):
        """Set up database tables for background tasks."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            # Create background tasks table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS background_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL,
                task_type TEXT NOT NULL,
                query TEXT,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                result_path TEXT,
                error_message TEXT,
                email_notification BOOLEAN DEFAULT 0,
                email TEXT,
                FOREIGN KEY (username) REFERENCES users(username)
            )
            ''')
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error setting up background tables: {e}")
    
    def render_background_processor_interface(self):
        """Display the background task manager interface."""
        st.header("Background Tasks")
        
        # Get all background tasks for the current user
        background_tasks = self.get_background_tasks()
        
        if background_tasks:
            # Convert to DataFrame for display
            tasks_df = pd.DataFrame(background_tasks)
            
            # Format timestamps
            for col in ['created_at', 'started_at', 'completed_at']:
                if col in tasks_df.columns:
                    tasks_df[col] = tasks_df[col].apply(
                        lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S") if x else "N/A"
                    )
            
            # Format status with emojis
            tasks_df['status'] = tasks_df['status'].apply(
                lambda x: {
                    'queued': '⏳ Queued',
                    'running': '🔄 Running',
                    'completed': '✅ Completed',
                    'failed': '❌ Failed'
                }.get(x, x)
            )
            
            # Display tasks
            st.dataframe(tasks_df)
            
            # Select a task to view details
            if len(background_tasks) > 0:
                selected_task_idx = st.selectbox(
                    "Select a task to view details:",
                    range(len(background_tasks)),
                    format_func=lambda i: f"{background_tasks[i]['task_type']} - {background_tasks[i]['status']} ({datetime.fromtimestamp(background_tasks[i]['created_at']).strftime('%Y-%m-%d %H:%M:%S')})"
                )
                
                selected_task = background_tasks[selected_task_idx]
                
                # Display task details
                st.subheader("Task Details")
                
                col1, col2 = st.columns(2)
                with col1:
                    st.write(f"**ID:** {selected_task['id']}")
                    st.write(f"**Type:** {selected_task['task_type']}")
                    st.write(f"**Status:** {selected_task['status']}")
                    st.write(f"**Created:** {datetime.fromtimestamp(selected_task['created_at']).strftime('%Y-%m-%d %H:%M:%S')}")
                
                with col2:
                    if selected_task['started_at']:
                        st.write(f"**Started:** {datetime.fromtimestamp(selected_task['started_at']).strftime('%Y-%m-%d %H:%M:%S')}")
                    if selected_task['completed_at']:
                        st.write(f"**Completed:** {datetime.fromtimestamp(selected_task['completed_at']).strftime('%Y-%m-%d %H:%M:%S')}")
                    
                    duration = None
                    if selected_task['started_at'] and selected_task['completed_at']:
                        duration = selected_task['completed_at'] - selected_task['started_at']
                        st.write(f"**Duration:** {duration:.2f} seconds")
                
                # Display query
                if selected_task['query']:
                    st.subheader("Query")
                    st.code(selected_task['query'], language="sql")
                
                # Display error if any
                if selected_task['error_message']:
                    st.error(f"Error: {selected_task['error_message']}")
                
                # Display results if completed
                if selected_task['status'] == 'completed' and selected_task['result_path']:
                    st.subheader("Results")
                    try:
                        results = pd.read_csv(selected_task['result_path'])
                        st.dataframe(results)
                        
                        # Download button
                        csv = results.to_csv(index=False)
                        st.download_button(
                            "Download Results CSV",
                            csv,
                            f"task_{selected_task['id']}_results.csv",
                            "text/csv",
                            key=f"download_task_{selected_task['id']}"
                        )
                    except Exception as e:
                        st.error(f"Could not load results: {e}")
        else:
            st.info("No background tasks found. Run a long query in the background to see it here.")
        
        # Create a new background task section
        st.subheader("Run a Query in Background")
        
        query = st.text_area("SQL Query:")
        notify_email = st.checkbox("Notify by email when completed")
        
        email = ""
        if notify_email:
            email = st.text_input("Email address:")
        
        if st.button("Run in Background"):
            if query:
                task_id = self.submit_background_query(query, email if notify_email else None)
                if task_id:
                    st.success(f"Query submitted for background processing (Task ID: {task_id})")
                else:
                    st.error("Failed to submit query for background processing")
            else:
                st.warning("Please enter a query to run in the background")
    
    def get_background_tasks(self):
        """Get all background tasks for the current user."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            cursor.execute(
                """
                SELECT id, username, task_type, query, status, created_at, 
                       started_at, completed_at, result_path, error_message, 
                       email_notification, email
                FROM background_tasks
                WHERE username = ?
                ORDER BY created_at DESC
                """,
                (st.session_state.username,)
            )
            
            tasks = cursor.fetchall()
            conn.close()
            
            # Convert to list of dictionaries
            return [
                {
                    "id": task[0],
                    "username": task[1],
                    "task_type": task[2],
                    "query": task[3],
                    "status": task[4],
                    "created_at": task[5],
                    "started_at": task[6],
                    "completed_at": task[7],
                    "result_path": task[8],
                    "error_message": task[9],
                    "email_notification": bool(task[10]),
                    "email": task[11]
                }
                for task in tasks
            ]
        except Exception as e:
            print(f"Error getting background tasks: {e}")
            return []
    
    def submit_background_query(self, query, email=None):
        """Submit a query to be executed in the background."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            # Insert the task
            cursor.execute(
                """
                INSERT INTO background_tasks 
                (username, task_type, query, status, created_at, email_notification, email) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    st.session_state.username,
                    "sql_query",
                    query,
                    "queued",
                    time.time(),
                    1 if email else 0,
                    email
                )
            )
            
            conn.commit()
            
            # Get the task ID
            task_id = cursor.lastrowid
            conn.close()
            
            # Start a background thread to execute the query
            thread = threading.Thread(
                target=self._execute_background_query,
                args=(task_id, query, email)
            )
            thread.daemon = True
            thread.start()
            
            # Create a notification
            from notification_manager import NotificationManager
            notification = {
                "type": "background_task",
                "message": f"Background query task #{task_id} has been queued",
                "timestamp": time.time(),
                "username": st.session_state.username
            }
            NotificationManager.add_notification(notification)
            
            return task_id
        except Exception as e:
            print(f"Error submitting background query: {e}")
            return None
    
    def _execute_background_query(self, task_id, query, email=None):
        """Execute a query in the background and update its status."""
        try:
            # Update task status to running
            self._update_task_status(task_id, "running", started_at=time.time())
            
            # Connect to database
            from db_manager import DatabaseManager
            db_manager = DatabaseManager()
            db_conn = db_manager.connect_to_mysql_db()  # Default to MySQL
            
            if not db_conn:
                self._update_task_status(
                    task_id, 
                    "failed", 
                    error_message="Failed to connect to database"
                )
                return
            
            # Execute the query
            cursor = db_conn.cursor()
            start_time = time.time()
            cursor.execute(query)
            
            # Fetch results
            results = cursor.fetchall()
            execution_time = time.time() - start_time
            
            # Close database connection
            db_conn.close()
            
            # Save results to CSV file
            if results:
                column_names = [desc[0] for desc in cursor.description]
                results_df = pd.DataFrame(results, columns=column_names)
                
                # Create results directory if it doesn't exist
                os.makedirs("results", exist_ok=True)
                
                # Save results to CSV
                result_path = f"results/task_{task_id}_results.csv"
                results_df.to_csv(result_path, index=False)
                
                # Update task status to completed
                self._update_task_status(
                    task_id, 
                    "completed", 
                    completed_at=time.time(),
                    result_path=result_path
                )
                
                # Send email notification if requested
                if email:
                    self._send_completion_email(task_id, email, execution_time, len(results_df))
                
                # Create a notification
                from notification_manager import NotificationManager
                notification = {
                    "type": "background_task",
                    "message": f"Background query task #{task_id} completed in {execution_time:.2f} seconds with {len(results_df)} results",
                    "timestamp": time.time(),
                    "username": st.session_state.username
                }
                NotificationManager.add_notification(notification)
            else:
                # Update task status to completed (no results)
                self._update_task_status(
                    task_id, 
                    "completed", 
                    completed_at=time.time()
                )
                
                # Send email notification if requested
                if email:
                    self._send_completion_email(task_id, email, execution_time, 0)
                
                # Create a notification
                from notification_manager import NotificationManager
                notification = {
                    "type": "background_task",
                    "message": f"Background query task #{task_id} completed in {execution_time:.2f} seconds with no results",
                    "timestamp": time.time(),
                    "username": st.session_state.username
                }
                NotificationManager.add_notification(notification)
                
        except Exception as e:
            # Update task status to failed
            self._update_task_status(
                task_id, 
                "failed", 
                completed_at=time.time(),
                error_message=str(e)
            )
            
            # Send email notification if requested
            if email:
                self._send_failure_email(task_id, email, str(e))
            
            # Create a notification
            from notification_manager import NotificationManager
            notification = {
                "type": "background_task",
                "message": f"Background query task #{task_id} failed: {str(e)}",
                "timestamp": time.time(),
                "username": st.session_state.username
            }
            NotificationManager.add_notification(notification)
    
    def _update_task_status(self, task_id, status, started_at=None, completed_at=None, result_path=None, error_message=None):
        """Update the status of a background task."""
        try:
            conn = sqlite3.connect("sql_gui.db")
            cursor = conn.cursor()
            
            # Build the update query
            update_fields = ["status = ?"]
            update_values = [status]
            
            if started_at is not None:
                update_fields.append("started_at = ?")
                update_values.append(started_at)
            
            if completed_at is not None:
                update_fields.append("completed_at = ?")
                update_values.append(completed_at)
            
            if result_path is not None:
                update_fields.append("result_path = ?")
                update_values.append(result_path)
            
            if error_message is not None:
                update_fields.append("error_message = ?")
                update_values.append(error_message)
            
            # Add task_id to values
            update_values.append(task_id)
            
            # Execute update
            cursor.execute(
                f"UPDATE background_tasks SET {', '.join(update_fields)} WHERE id = ?",
                update_values
            )
            
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error updating task status: {e}")
    
    def _send_completion_email(self, task_id, email, execution_time, row_count):
        """Send an email notification for a completed task."""
        # Use EmailService to send notification
        try:
            # Get notification settings
            self.email_service.send_query_completion_notification(email, task_id, execution_time, row_count)
            print(f"Email notification sent to {email} for task {task_id}")
        except Exception as e:
            print(f"Error sending email notification: {e}")
    
    def _send_failure_email(self, task_id, email, error_message):
        """Send an email notification for a failed task."""
        try:
            # Use EmailService to send notification
            self.email_service.send_query_failure_notification(email, task_id, error_message)
            print(f"Failure email notification sent to {email} for task {task_id}")
        except Exception as e:
            print(f"Error sending failure email notification: {e}")
