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
            is_approved BOOLEAN DEFAULT 1,
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
                    
                    # Handle missing values with optimized code
                    missing_strategy = st.selectbox(
                        "How to handle missing values:",
                        ["Drop rows with any missing values", 
                         "Fill numeric with 0, text with empty string", 
                         "Fill with mean/mode (where possible)", 
                         "Keep as is"],
                        key=f"missing_{table_name}"
                    )
                    
                    # Show a progress bar for data processing
                    progress_bar = st.progress(0)
                    st.write("Processing data...")
                    
                    total_cols = len(data.columns)
                    
                    # Process the data based on selected strategy
                    if missing_strategy == "Drop rows with any missing values":
                        # This is already optimized as it uses vectorized operations
                        data = data.dropna()
                        progress_bar.progress(1.0)
                        st.info(f"Dropped rows with missing values. New shape: {data.shape}")
                    
                    elif missing_strategy == "Fill numeric with 0, text with empty string":
                        # Identify numeric and non-numeric columns first
                        numeric_cols = data.select_dtypes(include=['number']).columns
                        non_numeric_cols = data.select_dtypes(exclude=['number']).columns
                        
                        # Use vectorized operations instead of loops
                        if len(numeric_cols) > 0:
                            data[numeric_cols] = data[numeric_cols].fillna(0)
                            progress_bar.progress(0.5)
                        
                        if len(non_numeric_cols) > 0:
                            data[non_numeric_cols] = data[non_numeric_cols].fillna("")
                            progress_bar.progress(1.0)
                        else:
                            progress_bar.progress(1.0)
                            
                        st.info("Filled missing values with 0 or empty string.")
                    
                    elif missing_strategy == "Fill with mean/mode (where possible)":
                        # Identify numeric columns first
                        numeric_cols = data.select_dtypes(include=['number']).columns
                        non_numeric_cols = data.select_dtypes(exclude=['number']).columns
                        
                        # Process numeric columns with vectorized operations
                        if len(numeric_cols) > 0:
                            # Calculate means for all numeric columns at once
                            means = data[numeric_cols].mean()
                            for i, col in enumerate(numeric_cols):
                                data[col] = data[col].fillna(means[col])
                                progress_bar.progress((i + 1) / (len(numeric_cols) + len(non_numeric_cols)))
                        
                        # Process non-numeric columns
                        if len(non_numeric_cols) > 0:
                            # Pre-calculate modes for all non-numeric columns
                            modes = {}
                            for col in non_numeric_cols:
                                mode_values = data[col].mode()
                                modes[col] = mode_values[0] if not mode_values.empty else ""
                            
                            # Fill with modes
                            for i, col in enumerate(non_numeric_cols):
                                data[col] = data[col].fillna(modes[col])
                                current_progress = (len(numeric_cols) + i + 1) / (len(numeric_cols) + len(non_numeric_cols))
                                progress_bar.progress(min(current_progress, 1.0))
                        
                        # Ensure progress bar reaches 100%
                        progress_bar.progress(1.0)
                        st.info("Filled missing values with mean/mode.")
                    else:
                        # No processing needed for "Keep as is"
                        progress_bar.progress(1.0)
                    
                    # Convert mixed-type columns to string - optimize this operation
                    object_cols = data.select_dtypes(include=['object']).columns
                    if len(object_cols) > 0:
                        data[object_cols] = data[object_cols].astype(str)
                    
                    # Clear the progress bar
                    time.sleep(0.5)  # Small delay to show completion
                    progress_bar.empty()
                    
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
        st.subheader("Scheduled Automated Uploads")
        
        # Check if the user is admin - if so, show the approval interface
        is_admin = st.session_state.role == "admin"
        
        # Admin approval interface
        if is_admin:
            tab1, tab2, tab3 = st.tabs(["Your Scheduled Uploads", "Pending Approval Requests", "Create New"])
            
            with tab1:
                self._render_user_scheduled_uploads()
                
            with tab2:
                self._render_admin_approval_interface()
                
            with tab3:
                self._render_create_scheduled_upload_form()
        else:
            # Regular user interface
            tab1, tab2 = st.tabs(["Your Scheduled Uploads", "Request New Upload"])
            
            with tab1:
                self._render_user_scheduled_uploads()
                
            with tab2:
                st.info("Scheduled uploads require admin approval. Complete the form below to submit a request.")
                self._render_create_scheduled_upload_form()
        
    def _render_user_scheduled_uploads(self):
        """Display interface for managing a user's scheduled uploads."""
        # Display existing scheduled uploads
        st.write("Current Scheduled Uploads:")
        scheduled_uploads = self.get_scheduled_uploads()
        
        if scheduled_uploads:
            # Add status labels for clarity
            for upload in scheduled_uploads:
                if not upload["is_approved"]:
                    upload["status"] = "Pending Approval"
                elif not upload["is_active"]:
                    upload["status"] = "Inactive"
                else:
                    upload["status"] = "Active"
                    
            # Create a display dataframe without credentials for security
            display_columns = ["id", "source_type", "table_name", "frequency", "next_run", 
                               "last_run", "status", "created_at"]
            display_df = pd.DataFrame([{k: upload[k] for k in display_columns if k in upload} 
                                      for upload in scheduled_uploads])
            
            st.dataframe(display_df)
            
            # Only show management interface if there are approved uploads
            if len(scheduled_uploads) > 0:
                # Allow disabling/enabling scheduled uploads
                st.subheader("Manage Scheduled Uploads")
                selected_upload = st.selectbox(
                    "Select a scheduled upload to manage:",
                    range(len(scheduled_uploads)),
                    format_func=lambda i: f"{scheduled_uploads[i]['table_name']} from {scheduled_uploads[i]['source_type']} (Status: {scheduled_uploads[i]['status']})"
                )
                
                upload_id = scheduled_uploads[selected_upload]['id']
                is_active = scheduled_uploads[selected_upload]['is_active']
                is_approved = scheduled_uploads[selected_upload]['is_approved']
                
                if is_approved:
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
                else:
                    st.warning("This upload is pending admin approval and cannot be modified yet.")
                
                if st.button(f"Delete Schedule for {scheduled_uploads[selected_upload]['table_name']}"):
                    self.delete_scheduled_upload(upload_id)
                    st.success(f"Scheduled upload for {scheduled_uploads[selected_upload]['table_name']} has been deleted.")
                    st.rerun()
        else:
            st.info("You don't have any scheduled uploads. You can create a new one using the form.")
        
    def _render_admin_approval_interface(self):
        """Display interface for admin approval of scheduled uploads."""
        # Only accessible by admins
        if st.session_state.role != "admin":
            return
            
        # Get all pending upload requests
        pending_requests = self.get_pending_upload_requests()
        
        if pending_requests:
            st.subheader("Pending Approval Requests")
            
            for i, request in enumerate(pending_requests):
                with st.expander(f"Request #{i+1}: {request['table_name']} by {request['username']}"):
                    st.write(f"**Source Type:** {request['source_type']}")
                    st.write(f"**Source Path:** {request['source_path']}")
                    st.write(f"**Frequency:** {request['frequency']}")
                    st.write(f"**Requested Start:** {request['next_run']}")
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if st.button(f"Approve", key=f"approve_{request['id']}"):
                            success, message = self.approve_upload_request(request['id'])
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)
                    
                    with col2:
                        reason = st.text_input("Reason for declining (optional):", key=f"reason_{request['id']}")
                        if st.button(f"Decline", key=f"decline_{request['id']}"):
                            success, message = self.decline_upload_request(request['id'], reason)
                            if success:
                                st.success(message)
                                st.rerun()
                            else:
                                st.error(message)
        else:
            st.info("No pending approval requests.")
                
    def _render_create_scheduled_upload_form(self):
        """Display form for creating/requesting new scheduled uploads."""
        # Form to create new scheduled upload
        st.subheader("Create New Scheduled Upload")
        
        is_admin = st.session_state.role == "admin"
        button_text = "Create Schedule" if is_admin else "Submit Request"
        
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
            
            submit_button = st.form_submit_button(button_text)
            
            if submit_button:
                # Combine date and time to get next run timestamp
                next_run = datetime.combine(start_date, start_time)
                
                # Create new scheduled upload
                success, message = self.create_scheduled_upload(
                    source_type, source_path, table_name, frequency,
                    next_run.strftime("%Y-%m-%d %H:%M:%S"), credentials
                )
                
                if success:
                    st.success(message)
                    st.rerun()
                else:
                    st.error(message)
    
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
        """Get the upload history based on user role."""
        try:
            # Check if the user is admin - if so, show all uploads
            is_admin = st.session_state.role == "admin"
            
            if is_admin:
                # Admin sees all uploads
                self.cursor.execute(
                    "SELECT username, file_name, table_name, rows_count, timestamp FROM upload_history ORDER BY timestamp DESC LIMIT 100"
                )
            else:
                # Regular users only see their own uploads
                self.cursor.execute(
                    "SELECT username, file_name, table_name, rows_count, timestamp FROM upload_history WHERE username = ? ORDER BY timestamp DESC LIMIT 100",
                    (st.session_state.username,)
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
        """Create a new scheduled upload task or request approval if current user is not an admin."""
        try:
            # Check if user is admin - admins can create scheduled tasks directly
            is_admin = st.session_state.role == "admin"
            
            if is_admin:
                # Admin directly creates a scheduled upload
                self.cursor.execute(
                    """
                    INSERT INTO scheduled_uploads 
                    (username, source_type, source_path, table_name, frequency, next_run, credentials, is_approved) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (st.session_state.username, source_type, source_path, table_name, frequency, next_run, credentials, 1)
                )
                self.conn.commit()
                
                # Register the task with the scheduler (since it's pre-approved)
                from scheduler_manager import SchedulerManager
                scheduler = SchedulerManager(self.conn)
                scheduler.register_task(source_type, source_path, table_name, frequency, next_run, credentials)
                
                return True, "Scheduled upload created successfully"
            else:
                # Regular user submits an approval request
                # First, check if there's a pending request for the same table
                self.cursor.execute(
                    """
                    SELECT COUNT(*) FROM scheduled_uploads 
                    WHERE username = ? AND table_name = ? AND is_approved = 0
                    """,
                    (st.session_state.username, table_name)
                )
                
                count = self.cursor.fetchone()[0]
                if count > 0:
                    return False, "You already have a pending request for this table. Please wait for admin approval."
                
                # Create a new pending request
                self.cursor.execute(
                    """
                    INSERT INTO scheduled_uploads 
                    (username, source_type, source_path, table_name, frequency, next_run, credentials, is_approved, is_active) 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (st.session_state.username, source_type, source_path, table_name, frequency, next_run, credentials, 0, 0)
                )
                self.conn.commit()
                
                # Notify admin of the new request
                from notification_manager import NotificationManager
                NotificationManager.add_notification({
                    "type": "approval_request",
                    "message": f"User {st.session_state.username} requested approval for scheduled upload of table {table_name}",
                    "timestamp": time.time(),
                    "username": "admin"  # This ensures admin sees the notification
                })
                
                return True, "Your scheduled upload request has been submitted for admin approval"
        except Exception as e:
            st.error(f"Error creating scheduled upload request: {e}")
            return False, f"Error: {str(e)}"
    
    def get_scheduled_uploads(self):
        """Get all scheduled uploads for the current user."""
        try:
            self.cursor.execute(
                """
                SELECT id, source_type, source_path, table_name, frequency, 
                       next_run, is_active, last_run, created_at, is_approved 
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
                    "created_at": row[8],
                    "is_approved": bool(row[9]) if row[9] is not None else True  # Default to True for backward compatibility
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
            
    def get_pending_upload_requests(self):
        """Get all pending upload requests for admin approval."""
        # Only accessible by admins
        if st.session_state.role != "admin":
            return []
            
        try:
            self.cursor.execute(
                """
                SELECT id, username, source_type, source_path, table_name, frequency, next_run
                FROM scheduled_uploads 
                WHERE is_approved = 0
                ORDER BY created_at DESC
                """
            )
            requests = self.cursor.fetchall()
            
            # Format as a list of dictionaries
            return [
                {
                    "id": row[0],
                    "username": row[1],
                    "source_type": row[2],
                    "source_path": row[3],
                    "table_name": row[4],
                    "frequency": row[5],
                    "next_run": row[6]
                }
                for row in requests
            ]
        except Exception as e:
            st.error(f"Error retrieving pending upload requests: {e}")
            return []
            
    def approve_upload_request(self, request_id):
        """Approve a scheduled upload request."""
        # Only accessible by admins
        if st.session_state.role != "admin":
            return False, "You don't have permission to approve upload requests"
            
        try:
            # Get request details
            self.cursor.execute(
                """
                SELECT username, source_type, source_path, table_name, frequency, next_run, credentials
                FROM scheduled_uploads 
                WHERE id = ?
                """,
                (request_id,)
            )
            request = self.cursor.fetchone()
            
            if not request:
                return False, "Request not found"
                
            username, source_type, source_path, table_name, frequency, next_run, credentials = request
            
            # Update request status
            self.cursor.execute(
                """
                UPDATE scheduled_uploads
                SET is_approved = 1, is_active = 1
                WHERE id = ?
                """,
                (request_id,)
            )
            self.conn.commit()
            
            # Register the task with the scheduler
            from scheduler_manager import SchedulerManager
            scheduler = SchedulerManager(self.conn)
            scheduler.register_task(source_type, source_path, table_name, frequency, next_run, credentials)
            
            # Send notification to the user
            from notification_manager import NotificationManager
            NotificationManager.add_notification({
                "type": "approval_approved",
                "message": f"Your scheduled upload request for table {table_name} has been approved",
                "timestamp": time.time(),
                "username": username
            })
            
            return True, f"Request approved. User {username} has been notified."
        except Exception as e:
            st.error(f"Error approving upload request: {e}")
            return False, f"Error: {str(e)}"
            
    def decline_upload_request(self, request_id, reason=""):
        """Decline a scheduled upload request."""
        # Only accessible by admins
        if st.session_state.role != "admin":
            return False, "You don't have permission to decline upload requests"
            
        try:
            # Get request details
            self.cursor.execute(
                """
                SELECT username, table_name
                FROM scheduled_uploads 
                WHERE id = ?
                """,
                (request_id,)
            )
            request = self.cursor.fetchone()
            
            if not request:
                return False, "Request not found"
                
            username, table_name = request
            
            # Delete the request
            self.cursor.execute(
                """
                DELETE FROM scheduled_uploads
                WHERE id = ?
                """,
                (request_id,)
            )
            self.conn.commit()
            
            # Send notification to the user
            from notification_manager import NotificationManager
            decline_message = f"Your scheduled upload request for table {table_name} has been declined"
            if reason:
                decline_message += f": {reason}"
                
            NotificationManager.add_notification({
                "type": "approval_declined",
                "message": decline_message,
                "timestamp": time.time(),
                "username": username
            })
            
            return True, f"Request declined. User {username} has been notified."
        except Exception as e:
            st.error(f"Error declining upload request: {e}")
            return False, f"Error: {str(e)}"
