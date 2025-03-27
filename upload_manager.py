import streamlit as st
import pandas as pd
import time
import os
import json
from datetime import datetime
import requests
import sqlite3
from db_manager import DatabaseManager

class UploadManager:
    """Manages data uploads and scheduled uploads."""
    
    def __init__(self, sqlite_conn):
        self.conn = sqlite_conn
        self.cursor = self.conn.cursor()
        self.setup_upload_tables()
        self.db_manager = DatabaseManager()
    
    def setup_upload_tables(self):
        """Create tables for storing upload history and scheduled uploads."""
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS upload_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            file_name TEXT NOT NULL,
            table_name TEXT NOT NULL,
            rows_count INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS scheduled_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_path TEXT NOT NULL,
            table_name TEXT NOT NULL,
            frequency TEXT NOT NULL,
            next_run TIMESTAMP,
            is_active BOOLEAN DEFAULT 1,
            last_run TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            credentials TEXT,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.conn.commit()
    
    def render_upload_interface(self, db_connection):
        """Display the data upload interface."""
        st.header("Upload and Store Data")
        
        # Create tabs for different upload methods
        tab1, tab2, tab3 = st.tabs(["File Upload", "Scheduled Uploads", "Upload History"])
        
        with tab1:
            self.render_file_upload(db_connection)
        
        with tab2:
            self.render_scheduled_uploads()
        
        with tab3:
            self.render_upload_history()
    
    def render_file_upload(self, db_connection):
        """Display the file upload interface."""
        st.subheader("Upload Files")
        uploaded_files = st.file_uploader("Upload CSV or Excel files", type=["csv", "xlsx"], accept_multiple_files=True)
        
        if uploaded_files:
            for uploaded_file in uploaded_files:
                st.write(f"Processing: {uploaded_file.name}")
                
                try:
                    # Determine file type and read data
                    if uploaded_file.name.endswith(".csv"):
                        data = pd.read_csv(uploaded_file)
                    else:
                        data = pd.read_excel(uploaded_file)
                    
                    # Get table name
                    default_table_name = os.path.splitext(uploaded_file.name)[0].replace(" ", "_")
                    table_name = st.text_input(f"Enter Table Name for {uploaded_file.name}:", default_table_name)
                    table_name = table_name.strip().replace(" ", "_")
                    
                    # Preview data
                    st.write(f"Preview of {uploaded_file.name}:")
                    st.dataframe(data.head())
                    
                    # Show data info
                    st.write("Data Information:")
                    from io import StringIO
                    buffer = StringIO()
                    data.info(buf=buffer)
                    st.text(buffer.getvalue())
                    
                    # Process data
                    st.subheader("Data Processing")
                    
                    # Handle missing values
                    missing_strategy = st.selectbox(
                        "How to handle missing values:",
                        ["Drop rows with any missing values", 
                         "Fill numeric with 0, text with empty string", 
                         "Fill with mean/mode (where possible)", 
                         "Keep as is"],
                        key=f"missing_{table_name}"
                    )
                    
                    if missing_strategy == "Drop rows with any missing values":
                        data = data.dropna()
                        st.info(f"Dropped rows with missing values. New shape: {data.shape}")
                    elif missing_strategy == "Fill numeric with 0, text with empty string":
                        for col in data.columns:
                            if data[col].dtype in ["float64", "int64"]:
                                data[col] = data[col].fillna(0)
                            else:
                                data[col] = data[col].fillna("")
                        st.info("Filled missing values with 0 or empty string.")
                    elif missing_strategy == "Fill with mean/mode (where possible)":
                        for col in data.columns:
                            if data[col].dtype in ["float64", "int64"]:
                                data[col] = data[col].fillna(data[col].mean())
                            else:
                                data[col] = data[col].fillna(data[col].mode()[0] if not data[col].mode().empty else "")
                        st.info("Filled missing values with mean/mode.")
                    
                    # Convert mixed-type columns to string
                    for col in data.columns:
                        if data[col].dtype == "object":
                            data[col] = data[col].astype(str)
                    
                    # Store data button
                    if st.button(f"Store Data in Database", key=f"store_{table_name}"):
                        # Determine database type
                        if hasattr(db_connection, 'cmd_query'):  # MySQL
                            db_type = "mysql"
                        elif hasattr(db_connection, 'notices'):  # PostgreSQL
                            db_type = "postgresql"
                        else:  # SQLite
                            db_type = "sqlite"
                        
                        success = self.db_manager.store_data_in_db(db_connection, data, table_name, db_type)
                        
                        if success:
                            # Record in upload history
                            self.record_upload(uploaded_file.name, table_name, len(data))
                            
                            # Create notification
                            from notification_manager import NotificationManager
                            notification = {
                                "type": "data_upload",
                                "message": f"Table {table_name} was created/updated with {len(data)} rows by {st.session_state.username}",
                                "timestamp": time.time(),
                                "username": st.session_state.username
                            }
                            NotificationManager.add_notification(notification)
                        
                except Exception as e:
                    st.error(f"Error processing {uploaded_file.name}: {e}")
        else:
            st.info("Upload CSV or Excel files to process and store in the database.")
    
    def render_scheduled_uploads(self):
        """Display interface for setting up scheduled data uploads."""
        st.subheader("Schedule Automated Uploads")
        
        # Check if user has admin role
        if st.session_state.role != "admin":
            st.warning("Scheduled uploads require admin permissions.")
            return
        
        # Display existing scheduled uploads
        st.write("Current Scheduled Uploads:")
        scheduled_uploads = self.get_scheduled_uploads()
        
        if scheduled_uploads:
            scheduled_df = pd.DataFrame(scheduled_uploads)
            st.dataframe(scheduled_df)
            
            # Allow disabling/enabling scheduled uploads
            st.subheader("Manage Scheduled Uploads")
            selected_upload = st.selectbox(
                "Select a scheduled upload to manage:",
                range(len(scheduled_uploads)),
                format_func=lambda i: f"{scheduled_uploads[i]['table_name']} from {scheduled_uploads[i]['source_type']} (Next run: {scheduled_uploads[i]['next_run']})"
            )
            
            upload_id = scheduled_uploads[selected_upload]['id']
            is_active = scheduled_uploads[selected_upload]['is_active']
            
            if is_active:
                if st.button(f"Disable Schedule for {scheduled_uploads[selected_upload]['table_name']}"):
                    self.toggle_scheduled_upload(upload_id, False)
                    st.success(f"Scheduled upload for {scheduled_uploads[selected_upload]['table_name']} has been disabled.")
                    st.rerun()
            else:
                if st.button(f"Enable Schedule for {scheduled_uploads[selected_upload]['table_name']}"):
                    self.toggle_scheduled_upload(upload_id, True)
                    st.success(f"Scheduled upload for {scheduled_uploads[selected_upload]['table_name']} has been enabled.")
                    st.rerun()
            
            if st.button(f"Delete Schedule for {scheduled_uploads[selected_upload]['table_name']}"):
                self.delete_scheduled_upload(upload_id)
                st.success(f"Scheduled upload for {scheduled_uploads[selected_upload]['table_name']} has been deleted.")
                st.rerun()
        
        # Form to create new scheduled upload
        st.subheader("Create New Scheduled Upload")
        
        with st.form("new_schedule_form"):
            source_type = st.selectbox(
                "Data Source:",
                ["Google Drive", "Dropbox", "FTP Server", "URL"]
            )
            
            source_path = st.text_input("Source Path/URL:")
            
            if source_type in ["Google Drive", "Dropbox", "FTP Server"]:
                credentials = st.text_area(
                    "Credentials (JSON format):",
                    help="Enter your API keys or credentials as a JSON object. For security, these will be encrypted."
                )
            else:
                credentials = "{}"
            
            table_name = st.text_input("Target Table Name:")
            
            frequency = st.selectbox(
                "Upload Frequency:",
                ["Daily", "Weekly", "Monthly", "Hourly"]
            )
            
            start_date = st.date_input("Start Date:")
            start_time = st.time_input("Start Time:")
            
            submit_button = st.form_submit_button("Create Schedule")
            
            if submit_button:
                # Combine date and time to get next run timestamp
                next_run = datetime.combine(start_date, start_time)
                
                # Create new scheduled upload
                success = self.create_scheduled_upload(
                    source_type, source_path, table_name, frequency,
                    next_run.strftime("%Y-%m-%d %H:%M:%S"), credentials
                )
                
                if success:
                    st.success(f"Scheduled upload created successfully. Next run: {next_run}")
                    st.rerun()
    
    def render_upload_history(self):
        """Display the history of data uploads."""
        st.subheader("Upload History")
        
        # Get upload history
        history = self.get_upload_history()
        
        if history:
            history_df = pd.DataFrame(history)
            st.dataframe(history_df)
            
            # Download history as CSV
            csv = history_df.to_csv(index=False)
            st.download_button(
                "Download History as CSV",
                csv,
                "upload_history.csv",
                "text/csv",
                key="download-history"
            )
        else:
            st.info("No upload history found.")
    
    def record_upload(self, file_name, table_name, rows_count):
        """Record an upload event in the history."""
        try:
            self.cursor.execute(
                "INSERT INTO upload_history (username, file_name, table_name, rows_count) VALUES (?, ?, ?, ?)",
                (st.session_state.username, file_name, table_name, rows_count)
            )
            self.conn.commit()
        except Exception as e:
            st.error(f"Error recording upload history: {e}")
    
    def get_upload_history(self):
        """Get the upload history for the current user."""
        try:
            self.cursor.execute(
                "SELECT username, file_name, table_name, rows_count, timestamp FROM upload_history ORDER BY timestamp DESC LIMIT 100"
            )
            history = self.cursor.fetchall()
            
            # Format as a list of dictionaries
            return [
                {
                    "username": row[0],
                    "file_name": row[1],
                    "table_name": row[2],
                    "rows_count": row[3],
                    "timestamp": row[4]
                }
                for row in history
            ]
        except Exception as e:
            st.error(f"Error retrieving upload history: {e}")
            return []
    
    def create_scheduled_upload(self, source_type, source_path, table_name, frequency, next_run, credentials):
        """Create a new scheduled upload task."""
        try:
            self.cursor.execute(
                """
                INSERT INTO scheduled_uploads 
                (username, source_type, source_path, table_name, frequency, next_run, credentials) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (st.session_state.username, source_type, source_path, table_name, frequency, next_run, credentials)
            )
            self.conn.commit()
            
            # Register the task with the scheduler
            from scheduler_manager import SchedulerManager
            scheduler = SchedulerManager(self.conn)
            scheduler.register_task(source_type, source_path, table_name, frequency, next_run, credentials)
            
            return True
        except Exception as e:
            st.error(f"Error creating scheduled upload: {e}")
            return False
    
    def get_scheduled_uploads(self):
        """Get all scheduled uploads for the current user."""
        try:
            self.cursor.execute(
                """
                SELECT id, source_type, source_path, table_name, frequency, 
                       next_run, is_active, last_run, created_at 
                FROM scheduled_uploads 
                WHERE username = ?
                ORDER BY next_run
                """,
                (st.session_state.username,)
            )
            uploads = self.cursor.fetchall()
            
            # Format as a list of dictionaries
            return [
                {
                    "id": row[0],
                    "source_type": row[1],
                    "source_path": row[2],
                    "table_name": row[3],
                    "frequency": row[4],
                    "next_run": row[5],
                    "is_active": bool(row[6]),
                    "last_run": row[7] if row[7] else "Never",
                    "created_at": row[8]
                }
                for row in uploads
            ]
        except Exception as e:
            st.error(f"Error retrieving scheduled uploads: {e}")
            return []
    
    def toggle_scheduled_upload(self, upload_id, is_active):
        """Enable or disable a scheduled upload."""
        try:
            self.cursor.execute(
                "UPDATE scheduled_uploads SET is_active = ? WHERE id = ?",
                (1 if is_active else 0, upload_id)
            )
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Error toggling scheduled upload: {e}")
            return False
    
    def delete_scheduled_upload(self, upload_id):
        """Delete a scheduled upload."""
        try:
            self.cursor.execute(
                "DELETE FROM scheduled_uploads WHERE id = ?",
                (upload_id,)
            )
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Error deleting scheduled upload: {e}")
            return False
