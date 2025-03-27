# This file configures Celery for background task processing
from celery import Celery
import os
import time
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

# Configure Celery
broker_url = 'filesystem://'
broker_dir = '/tmp/celery'
os.makedirs(f"{broker_dir}/out", exist_ok=True)
os.makedirs(f"{broker_dir}/processed", exist_ok=True)

app = Celery('sql_gui_tasks', 
             broker=broker_url,
             backend='sqlite:///celery_results.sqlite')

# Configure file-based broker
app.conf.update(
    broker_transport_options={
        'data_folder_in': f'{broker_dir}/out',
        'data_folder_out': f'{broker_dir}/out',
        'data_folder_processed': f'{broker_dir}/processed'
    },
    worker_concurrency=2,
    task_track_started=True,
    task_time_limit=3600,  # 1 hour time limit for tasks
    result_expires=timedelta(days=1)
)

@app.task
def execute_query(query, task_id, username, db_type="mysql"):
    """Execute a SQL query as a background task."""
    try:
        # Update task status to running
        update_task_status(task_id, "running", started_at=time.time())
        
        # Connect to database
        if db_type == "mysql":
            import mysql.connector
            conn = mysql.connector.connect(
                host=os.getenv("MYSQL_HOST", "localhost"),
                user=os.getenv("MYSQL_USER", "root"),
                password=os.getenv("MYSQL_PASSWORD", "root"),
                database=os.getenv("MYSQL_DATABASE", "gui"),
                auth_plugin="mysql_native_password",
                connection_timeout=10
            )
        elif db_type == "postgresql":
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv("PGHOST", "localhost"),
                user=os.getenv("PGUSER", "postgres"),
                password=os.getenv("PGPASSWORD", "postgres"),
                dbname=os.getenv("PGDATABASE", "postgres"),
                port=os.getenv("PGPORT", "5432")
            )
        elif db_type == "sqlite":
            conn = sqlite3.connect("database.db")
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        # Execute the query
        cursor = conn.cursor()
        start_time = time.time()
        cursor.execute(query)
        
        # Fetch results
        results = cursor.fetchall()
        execution_time = time.time() - start_time
        
        # Format results
        if results and cursor.description:
            column_names = [desc[0] for desc in cursor.description]
            results_df = pd.DataFrame(results, columns=column_names)
            
            # Create results directory if it doesn't exist
            os.makedirs("results", exist_ok=True)
            
            # Save results to CSV
            result_path = f"results/task_{task_id}_results.csv"
            results_df.to_csv(result_path, index=False)
            
            # Update task status to completed
            update_task_status(
                task_id, 
                "completed", 
                completed_at=time.time(),
                result_path=result_path
            )
            
            # Create notification
            create_task_notification(
                username,
                "background_task",
                f"Background query task #{task_id} completed in {execution_time:.2f} seconds with {len(results_df)} results"
            )
            
            # Send email notification if needed
            email = get_task_email(task_id)
            if email:
                send_completion_email(task_id, email, execution_time, len(results_df))
        else:
            # Update task status to completed (no results)
            update_task_status(
                task_id, 
                "completed", 
                completed_at=time.time()
            )
            
            # Create notification
            create_task_notification(
                username,
                "background_task",
                f"Background query task #{task_id} completed in {execution_time:.2f} seconds with no results"
            )
            
            # Send email notification if needed
            email = get_task_email(task_id)
            if email:
                send_completion_email(task_id, email, execution_time, 0)
        
        # Close connections
        cursor.close()
        conn.close()
        
        return {
            "task_id": task_id,
            "status": "completed",
            "execution_time": execution_time,
            "row_count": len(results) if results else 0
        }
    
    except Exception as e:
        # Update task status to failed
        update_task_status(
            task_id, 
            "failed", 
            completed_at=time.time(),
            error_message=str(e)
        )
        
        # Create notification
        create_task_notification(
            username,
            "background_task",
            f"Background query task #{task_id} failed: {str(e)}"
        )
        
        # Send email notification if needed
        email = get_task_email(task_id)
        if email:
            send_failure_email(task_id, email, str(e))
        
        return {
            "task_id": task_id,
            "status": "failed",
            "error": str(e)
        }

@app.task
def scheduled_data_upload(source_type, source_path, table_name, credentials, username):
    """Execute a scheduled data upload as a background task."""
    try:
        # Create notification
        create_task_notification(
            username,
            "scheduled_upload",
            f"Starting scheduled upload for table '{table_name}' from {source_type}"
        )
        
        # Download data from source
        data = download_data_from_source(source_type, source_path, credentials)
        
        if data is None:
            create_task_notification(
                username,
                "scheduled_upload",
                f"Failed to download data from {source_type} for table '{table_name}'"
            )
            return {
                "status": "failed",
                "error": "Failed to download data from source"
            }
        
        # Connect to database (default to MySQL)
        import mysql.connector
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "localhost"),
            user=os.getenv("MYSQL_USER", "root"),
            password=os.getenv("MYSQL_PASSWORD", "root"),
            database=os.getenv("MYSQL_DATABASE", "gui"),
            auth_plugin="mysql_native_password",
            connection_timeout=10
        )
        
        # Store data in database
        success = store_data_in_db(conn, data, table_name)
        
        if success:
            # Update last run timestamp
            update_scheduled_upload_last_run(source_type, table_name)
            
            # Create notification
            create_task_notification(
                username,
                "scheduled_upload",
                f"Scheduled upload completed: {len(data)} rows uploaded to '{table_name}' from {source_type}"
            )
            
            return {
                "status": "completed",
                "rows_uploaded": len(data)
            }
        else:
            create_task_notification(
                username,
                "scheduled_upload",
                f"Failed to store data in table '{table_name}'"
            )
            
            return {
                "status": "failed",
                "error": "Failed to store data in database"
            }
    
    except Exception as e:
        # Create notification
        create_task_notification(
            username,
            "scheduled_upload",
            f"Scheduled upload failed for table '{table_name}': {str(e)}"
        )
        
        return {
            "status": "failed",
            "error": str(e)
        }

def update_task_status(task_id, status, started_at=None, completed_at=None, result_path=None, error_message=None):
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

def get_task_email(task_id):
    """Get the email address for a task notification."""
    try:
        conn = sqlite3.connect("sql_gui.db")
        cursor = conn.cursor()
        
        cursor.execute(
            "SELECT email FROM background_tasks WHERE id = ? AND email_notification = 1",
            (task_id,)
        )
        
        result = cursor.fetchone()
        conn.close()
        
        return result[0] if result else None
    except Exception as e:
        print(f"Error getting task email: {e}")
        return None

def send_completion_email(task_id, email, execution_time, row_count):
    """Send an email notification for a completed task."""
    try:
        from email_service import EmailService
        email_service = EmailService()
        
        subject = f"SQL Query GUI - Background Task #{task_id} Completed"
        
        body = f"""
        <html>
        <body>
            <h2>Background Task Completed</h2>
            <p>Your background query task has been completed successfully.</p>
            <ul>
                <li><strong>Task ID:</strong> {task_id}</li>
                <li><strong>Execution Time:</strong> {execution_time:.2f} seconds</li>
                <li><strong>Results:</strong> {row_count} rows</li>
            </ul>
            <p>You can view the complete results in the SQL Query GUI application.</p>
        </body>
        </html>
        """
        
        email_service.send_email(email, subject, body)
    except Exception as e:
        print(f"Error sending completion email: {e}")

def send_failure_email(task_id, email, error_message):
    """Send an email notification for a failed task."""
    try:
        from email_service import EmailService
        email_service = EmailService()
        
        subject = f"SQL Query GUI - Background Task #{task_id} Failed"
        
        body = f"""
        <html>
        <body>
            <h2>Background Task Failed</h2>
            <p>Your background query task has failed to complete.</p>
            <ul>
                <li><strong>Task ID:</strong> {task_id}</li>
                <li><strong>Error:</strong> {error_message}</li>
            </ul>
            <p>Please check your query and try again.</p>
        </body>
        </html>
        """
        
        email_service.send_email(email, subject, body)
    except Exception as e:
        print(f"Error sending failure email: {e}")

def create_task_notification(username, notification_type, message):
    """Create a notification for a task event."""
    try:
        conn = sqlite3.connect("sql_gui.db")
        cursor = conn.cursor()
        
        cursor.execute(
            """
            INSERT INTO notifications (type, message, timestamp, username)
            VALUES (?, ?, ?, ?)
            """,
            (notification_type, message, time.time(), username)
        )
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error creating task notification: {e}")

def download_data_from_source(source_type, source_path, credentials):
    """Download data from a specified source."""
    try:
        from scheduler_manager import SchedulerManager
        scheduler = SchedulerManager()
        return scheduler.download_data_from_source(source_type, source_path, credentials)
    except Exception as e:
        print(f"Error downloading data: {e}")
        return None

def store_data_in_db(conn, data, table_name):
    """Store data in a database table."""
    try:
        from db_manager import DatabaseManager
        db_manager = DatabaseManager()
        return db_manager.store_data_in_db(conn, data, table_name)
    except Exception as e:
        print(f"Error storing data: {e}")
        return False

def update_scheduled_upload_last_run(source_type, table_name):
    """Update the last_run timestamp for a scheduled upload."""
    try:
        conn = sqlite3.connect("sql_gui.db")
        cursor = conn.cursor()
        
        cursor.execute(
            """
            UPDATE scheduled_uploads 
            SET last_run = ?
            WHERE source_type = ? AND table_name = ?
            """,
            (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), source_type, table_name)
        )
        
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"Error updating last_run: {e}")
        return False
