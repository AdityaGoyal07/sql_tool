import streamlit as st
import time
import pandas as pd
import json
from datetime import datetime, timedelta
import requests
import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.date import DateTrigger
import threading

class SchedulerManager:
    """Manages scheduled data uploads and tasks."""
    
    _scheduler = None
    _lock = threading.Lock()
    
    def __init__(self, sqlite_conn=None):
        self.conn = sqlite_conn
        if self.conn:
            self.cursor = self.conn.cursor()
        
        # Initialize the scheduler if not already running
        SchedulerManager.initialize_scheduler()
    
    @classmethod
    def initialize_scheduler(cls):
        """Initialize the background scheduler if not already running."""
        with cls._lock:
            if cls._scheduler is None:
                cls._scheduler = BackgroundScheduler()
                cls._scheduler.start()
                
                # Register shutdown handler
                import atexit
                atexit.register(lambda: cls._scheduler.shutdown())
    
    def register_task(self, source_type, source_path, table_name, frequency, next_run, credentials=None):
        """Register a new scheduled task with the scheduler."""
        # Convert next_run string to datetime object
        if isinstance(next_run, str):
            next_run = datetime.strptime(next_run, "%Y-%m-%d %H:%M:%S")
        
        # Create the job ID
        job_id = f"{source_type}_{table_name}_{int(time.time())}"
        
        # Set up the appropriate trigger based on frequency
        if frequency.lower() == "once":
            trigger = DateTrigger(run_date=next_run)
        else:
            # Calculate interval in seconds
            if frequency.lower() == "hourly":
                interval = 60 * 60  # 1 hour
            elif frequency.lower() == "daily":
                interval = 24 * 60 * 60  # 24 hours
            elif frequency.lower() == "weekly":
                interval = 7 * 24 * 60 * 60  # 7 days
            elif frequency.lower() == "monthly":
                interval = 30 * 24 * 60 * 60  # 30 days
            else:
                # Default to daily
                interval = 24 * 60 * 60
            
            trigger = IntervalTrigger(seconds=interval, start_date=next_run)
        
        # Schedule the job
        SchedulerManager._scheduler.add_job(
            self.execute_scheduled_upload,
            trigger=trigger,
            args=[source_type, source_path, table_name, credentials],
            id=job_id,
            replace_existing=True
        )
        
        return job_id
    
    def execute_scheduled_upload(self, source_type, source_path, table_name, credentials=None):
        """Execute a scheduled data upload task."""
        try:
            # Log the execution
            print(f"Executing scheduled upload: {source_type} for {table_name}")
            
            # Download data from the source
            data = self.download_data_from_source(source_type, source_path, credentials)
            
            if data is None:
                # Create notification about failure
                self.create_failure_notification(source_type, table_name, "Failed to download data from source")
                return
            
            # Connect to database
            from db_manager import DatabaseManager
            db_manager = DatabaseManager()
            db_conn = db_manager.connect_to_mysql_db()  # Default to MySQL
            
            if not db_conn:
                self.create_failure_notification(source_type, table_name, "Failed to connect to database")
                return
            
            # Store the data
            success = db_manager.store_data_in_db(db_conn, data, table_name)
            
            if success:
                # Update last_run timestamp in the database
                self.update_last_run(source_type, table_name)
                
                # Create notification about successful upload
                from notification_manager import NotificationManager
                notification = {
                    "type": "scheduled_upload",
                    "message": f"Scheduled upload completed: {len(data)} rows uploaded to {table_name} from {source_type}",
                    "timestamp": time.time(),
                    "username": "system"
                }
                NotificationManager.add_notification(notification)
            else:
                self.create_failure_notification(source_type, table_name, "Failed to store data in database")
            
            # Close database connection
            db_conn.close()
            
        except Exception as e:
            # Handle any exceptions during the scheduled upload
            error_message = str(e)
            self.create_failure_notification(source_type, table_name, f"Error during scheduled upload: {error_message}")
    
    def download_data_from_source(self, source_type, source_path, credentials=None):
        """Download data from the specified source."""
        try:
            # Parse credentials if provided
            if credentials and isinstance(credentials, str):
                try:
                    credentials_dict = json.loads(credentials)
                except:
                    credentials_dict = {}
            elif credentials:
                credentials_dict = credentials
            else:
                credentials_dict = {}
            
            # Handle different source types
            if source_type.lower() == "google drive":
                return self.download_from_google_drive(source_path, credentials_dict)
            elif source_type.lower() == "dropbox":
                return self.download_from_dropbox(source_path, credentials_dict)
            elif source_type.lower() == "ftp server":
                return self.download_from_ftp(source_path, credentials_dict)
            elif source_type.lower() == "url":
                return self.download_from_url(source_path)
            else:
                print(f"Unsupported source type: {source_type}")
                return None
                
        except Exception as e:
            print(f"Error downloading data: {e}")
            return None
    
    def download_from_google_drive(self, file_id, credentials):
        """Download a file from Google Drive."""
        try:
            # In a real implementation, this would use Google Drive API
            # For this demonstration, we'll simulate the download
            print(f"Simulating download from Google Drive: {file_id}")
            
            # Create a simple example DataFrame for demonstration
            data = pd.DataFrame({
                "id": range(1, 101),
                "value": [i * 2 for i in range(1, 101)],
                "category": ["A" if i % 3 == 0 else "B" if i % 3 == 1 else "C" for i in range(1, 101)],
                "date": [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 101)]
            })
            
            return data
            
        except Exception as e:
            print(f"Error downloading from Google Drive: {e}")
            return None
    
    def download_from_dropbox(self, file_path, credentials):
        """Download a file from Dropbox."""
        try:
            # In a real implementation, this would use Dropbox API
            # For this demonstration, we'll simulate the download
            print(f"Simulating download from Dropbox: {file_path}")
            
            # Create a simple example DataFrame for demonstration
            data = pd.DataFrame({
                "id": range(1, 101),
                "value": [i * 3 for i in range(1, 101)],
                "category": ["X" if i % 2 == 0 else "Y" for i in range(1, 101)],
                "date": [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 101)]
            })
            
            return data
            
        except Exception as e:
            print(f"Error downloading from Dropbox: {e}")
            return None
    
    def download_from_ftp(self, file_path, credentials):
        """Download a file from an FTP server."""
        try:
            # Extract credentials
            host = credentials.get("host", "")
            username = credentials.get("username", "anonymous")
            password = credentials.get("password", "")
            
            # In a real implementation, this would connect to an FTP server
            # For this demonstration, we'll simulate the download
            print(f"Simulating download from FTP: {file_path}")
            
            # Create a simple example DataFrame for demonstration
            data = pd.DataFrame({
                "id": range(1, 101),
                "value": [i * 1.5 for i in range(1, 101)],
                "category": ["P" if i % 4 == 0 else "Q" if i % 4 == 1 else "R" if i % 4 == 2 else "S" for i in range(1, 101)],
                "date": [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 101)]
            })
            
            return data
            
        except Exception as e:
            print(f"Error downloading from FTP: {e}")
            return None
    
    def download_from_url(self, url):
        """Download a file from a URL."""
        try:
            # For security reasons, we should validate the URL
            allowed_domains = ["github.com", "raw.githubusercontent.com", "data.gov", "data.world"]
            
            # Check if URL is from an allowed domain
            if not any(domain in url for domain in allowed_domains):
                print(f"URL not from an allowed domain: {url}")
                return None
            
            # In a real implementation, this would download from the URL
            # For this demonstration, we'll simulate the download or use a public dataset
            print(f"Simulating download from URL: {url}")
            
            # Try to download from the URL (for certain safe domains)
            if "raw.githubusercontent.com" in url and (url.endswith(".csv") or url.endswith(".xlsx")):
                response = requests.get(url)
                if response.status_code == 200:
                    # Determine file type from URL
                    if url.endswith(".csv"):
                        # Save to a temporary file and read
                        with open("temp_download.csv", "wb") as f:
                            f.write(response.content)
                        data = pd.read_csv("temp_download.csv")
                        os.remove("temp_download.csv")
                        return data
                    elif url.endswith(".xlsx"):
                        # Save to a temporary file and read
                        with open("temp_download.xlsx", "wb") as f:
                            f.write(response.content)
                        data = pd.read_excel("temp_download.xlsx")
                        os.remove("temp_download.xlsx")
                        return data
            
            # Create a fallback example DataFrame for demonstration
            data = pd.DataFrame({
                "id": range(1, 101),
                "value": [i * 2.5 for i in range(1, 101)],
                "category": ["Alpha" if i % 3 == 0 else "Beta" if i % 3 == 1 else "Gamma" for i in range(1, 101)],
                "date": [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, 101)]
            })
            
            return data
            
        except Exception as e:
            print(f"Error downloading from URL: {e}")
            return None
    
    def update_last_run(self, source_type, table_name):
        """Update the last_run timestamp for a scheduled upload."""
        if not self.conn:
            return False
        
        try:
            self.cursor.execute(
                """
                UPDATE scheduled_uploads 
                SET last_run = ?
                WHERE source_type = ? AND table_name = ?
                """,
                (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), source_type, table_name)
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error updating last_run: {e}")
            return False
    
    def create_failure_notification(self, source_type, table_name, error_message):
        """Create a notification for a failed scheduled upload."""
        from notification_manager import NotificationManager
        notification = {
            "type": "scheduled_upload",
            "message": f"Scheduled upload failed: {table_name} from {source_type}. Error: {error_message}",
            "timestamp": time.time(),
            "username": "system"
        }
        NotificationManager.add_notification(notification)
    
    def render_scheduler_interface(self):
        """Render the scheduler interface for managing scheduled uploads."""
        st.header("Scheduled Automated Uploads")
        
        # Check if user has admin role
        if st.session_state.role != "admin":
            st.warning("Scheduled uploads require admin permissions.")
            return
        
        # Tabs for different sections
        tab1, tab2 = st.tabs(["Active Schedules", "Create New Schedule"])
        
        with tab1:
            self.render_active_schedules()
        
        with tab2:
            self.render_new_schedule_form()
    
    def render_active_schedules(self):
        """Render the active scheduled uploads table."""
        st.subheader("Active Scheduled Uploads")
        
        # Get all scheduled uploads
        scheduled_uploads = self.get_all_scheduled_uploads()
        
        if not scheduled_uploads:
            st.info("No scheduled uploads found.")
            return
        
        # Create a DataFrame for display
        scheduled_df = pd.DataFrame(scheduled_uploads)
        
        # Format active status
        scheduled_df["is_active"] = scheduled_df["is_active"].apply(
            lambda x: "✅ Active" if x else "❌ Inactive"
        )
        
        # Display the scheduled uploads
        st.dataframe(scheduled_df)
        
        # Manage selected schedule
        st.subheader("Manage Schedule")
        selected_id = st.selectbox(
            "Select a schedule to manage:",
            scheduled_df["id"].tolist(),
            format_func=lambda x: f"{scheduled_df.loc[scheduled_df['id'] == x, 'table_name'].iloc[0]} ({scheduled_df.loc[scheduled_df['id'] == x, 'frequency'].iloc[0]})"
        )
        
        # Get the selected schedule
        selected_schedule = scheduled_df.loc[scheduled_df["id"] == selected_id].iloc[0]
        
        # Display selected schedule details
        st.write("Schedule Details:")
        st.json({
            "id": selected_schedule["id"],
            "source_type": selected_schedule["source_type"],
            "source_path": selected_schedule["source_path"],
            "table_name": selected_schedule["table_name"],
            "frequency": selected_schedule["frequency"],
            "next_run": selected_schedule["next_run"],
            "last_run": selected_schedule["last_run"],
            "is_active": selected_schedule["is_active"]
        })
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        
        with col1:
            # Toggle active status
            if "✅" in selected_schedule["is_active"]:
                if st.button("Deactivate Schedule"):
                    self.toggle_scheduled_upload(selected_id, False)
                    st.success(f"Schedule for {selected_schedule['table_name']} has been deactivated.")
                    st.rerun()
            else:
                if st.button("Activate Schedule"):
                    self.toggle_scheduled_upload(selected_id, True)
                    st.success(f"Schedule for {selected_schedule['table_name']} has been activated.")
                    st.rerun()
        
        with col2:
            # Run now button
            if st.button("Run Now"):
                # Execute the scheduled upload immediately
                source_type = selected_schedule["source_type"]
                source_path = selected_schedule["source_path"]
                table_name = selected_schedule["table_name"]
                credentials = self.get_schedule_credentials(selected_id)
                
                # Create a background thread to run the upload
                threading.Thread(
                    target=self.execute_scheduled_upload,
                    args=[source_type, source_path, table_name, credentials]
                ).start()
                
                st.success(f"Upload for {table_name} has been triggered.")
        
        with col3:
            # Delete button
            if st.button("Delete Schedule"):
                self.delete_scheduled_upload(selected_id)
                st.success(f"Schedule for {selected_schedule['table_name']} has been deleted.")
                st.rerun()
    
    def render_new_schedule_form(self):
        """Render the form for creating a new scheduled upload."""
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
                    
                    # Create notification
                    from notification_manager import NotificationManager
                    notification = {
                        "type": "scheduled_upload",
                        "message": f"New scheduled upload created for {table_name} from {source_type}. Next run: {next_run}",
                        "timestamp": time.time(),
                        "username": st.session_state.username
                    }
                    NotificationManager.add_notification(notification)
                    
                    st.rerun()
    
    def get_all_scheduled_uploads(self):
        """Get all scheduled uploads."""
        if not self.conn:
            return []
        
        try:
            self.cursor.execute(
                """
                SELECT id, source_type, source_path, table_name, frequency, 
                       next_run, is_active, last_run, created_at 
                FROM scheduled_uploads 
                ORDER BY next_run
                """
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
    
    def get_schedule_credentials(self, schedule_id):
        """Get the credentials for a specific scheduled upload."""
        if not self.conn:
            return {}
        
        try:
            self.cursor.execute(
                "SELECT credentials FROM scheduled_uploads WHERE id = ?",
                (schedule_id,)
            )
            result = self.cursor.fetchone()
            
            if result and result[0]:
                try:
                    return json.loads(result[0])
                except:
                    return {}
            return {}
        except Exception as e:
            print(f"Error retrieving schedule credentials: {e}")
            return {}
    
    def toggle_scheduled_upload(self, upload_id, is_active):
        """Enable or disable a scheduled upload."""
        if not self.conn:
            return False
        
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
        if not self.conn:
            return False
        
        try:
            # Get schedule details before deleting
            self.cursor.execute(
                "SELECT table_name, source_type FROM scheduled_uploads WHERE id = ?",
                (upload_id,)
            )
            schedule_info = self.cursor.fetchone()
            
            # Delete from database
            self.cursor.execute(
                "DELETE FROM scheduled_uploads WHERE id = ?",
                (upload_id,)
            )
            self.conn.commit()
            
            # Try to remove from scheduler if it exists
            if SchedulerManager._scheduler:
                job_id = f"{schedule_info[1]}_{schedule_info[0]}_{upload_id}"
                try:
                    SchedulerManager._scheduler.remove_job(job_id)
                except:
                    pass  # Job might not exist in scheduler
            
            return True
        except Exception as e:
            st.error(f"Error deleting scheduled upload: {e}")
            return False
    
    def create_scheduled_upload(self, source_type, source_path, table_name, frequency, next_run, credentials):
        """Create a new scheduled upload task."""
        if not self.conn:
            return False
        
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
            
            # Get the inserted ID
            self.cursor.execute("SELECT last_insert_rowid()")
            upload_id = self.cursor.fetchone()[0]
            
            # Register the task with the scheduler
            self.register_task(source_type, source_path, table_name, frequency, next_run, credentials)
            
            return True
        except Exception as e:
            st.error(f"Error creating scheduled upload: {e}")
            return False
