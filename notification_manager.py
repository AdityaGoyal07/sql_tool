import streamlit as st
import pandas as pd
import time
import json
from datetime import datetime
import sqlite3

class NotificationManager:
    """Manages real-time notifications for data changes and long-running queries."""
    
    def __init__(self, sqlite_conn):
        self.conn = sqlite_conn
        self.cursor = self.conn.cursor()
        self.setup_notification_tables()
    
    def setup_notification_tables(self):
        """Create tables for storing notifications."""
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS notifications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            message TEXT NOT NULL,
            timestamp REAL NOT NULL,
            username TEXT,
            is_read INTEGER DEFAULT 0,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS notification_settings (
            username TEXT PRIMARY KEY,
            data_changes BOOLEAN DEFAULT 1,
            long_queries BOOLEAN DEFAULT 1,
            new_tables BOOLEAN DEFAULT 1,
            email_alerts BOOLEAN DEFAULT 0,
            email TEXT,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.conn.commit()
    
    @staticmethod
    def add_notification(notification_data):
        """Add a notification to the queue and database."""
        if 'notification_queue' not in st.session_state:
            st.session_state.notification_queue = []
        
        # Add to session state queue
        st.session_state.notification_queue.append(notification_data)
        
        # Also add to database for persistence
        try:
            sqlite_conn = sqlite3.connect("sql_gui.db")
            cursor = sqlite_conn.cursor()
            
            cursor.execute(
                "INSERT INTO notifications (type, message, timestamp, username) VALUES (?, ?, ?, ?)",
                (
                    notification_data["type"],
                    notification_data["message"],
                    notification_data["timestamp"],
                    notification_data.get("username", "system")
                )
            )
            sqlite_conn.commit()
            sqlite_conn.close()
        except Exception as e:
            print(f"Error storing notification: {e}")
    
    def check_notifications(self):
        """Check for new notifications and display them."""
        if 'last_notification_check' not in st.session_state:
            st.session_state.last_notification_check = time.time()
        
        # Fetch new notifications from database
        try:
            self.cursor.execute(
                """
                SELECT id, type, message, timestamp 
                FROM notifications 
                WHERE timestamp > ? AND is_read = 0
                ORDER BY timestamp DESC
                LIMIT 5
                """,
                (st.session_state.last_notification_check,)
            )
            
            new_notifications = self.cursor.fetchall()
            
            # Update last check time
            st.session_state.last_notification_check = time.time()
            
            # Display new notifications
            for notification in new_notifications:
                notification_id, notification_type, message, timestamp = notification
                
                # Format timestamp
                notification_time = datetime.fromtimestamp(timestamp).strftime("%H:%M:%S")
                
                # Determine notification style based on type
                if notification_type == "data_upload":
                    st.info(f"{notification_time} - 📊 {message}")
                elif notification_type == "new_table":
                    st.success(f"{notification_time} - 🆕 {message}")
                elif notification_type == "long_query":
                    st.warning(f"{notification_time} - ⏱️ {message}")
                elif notification_type == "database_reset":
                    st.error(f"{notification_time} - 🗑️ {message}")
                else:
                    st.info(f"{notification_time} - {message}")
                
                # Mark notification as read
                self.mark_notification_read(notification_id)
                
        except Exception as e:
            st.error(f"Error checking notifications: {e}")
    
    def mark_notification_read(self, notification_id):
        """Mark a notification as read."""
        try:
            self.cursor.execute(
                "UPDATE notifications SET is_read = 1 WHERE id = ?",
                (notification_id,)
            )
            self.conn.commit()
        except Exception as e:
            st.error(f"Error marking notification as read: {e}")
    
    def render_notification_interface(self):
        """Display the notification settings and history interface."""
        st.header("Notifications")
        
        tab1, tab2 = st.tabs(["Notification History", "Notification Settings"])
        
        with tab1:
            self.render_notification_history()
        
        with tab2:
            self.render_notification_settings()
    
    def render_notification_history(self):
        """Display the notification history based on user role."""
        st.subheader("Notification History")
        
        # Check if the user is admin - if so, show all notifications
        is_admin = st.session_state.role == "admin"
        
        # Fetch notification history from database based on role
        try:
            if is_admin:
                # Admin sees all notifications
                self.cursor.execute(
                    """
                    SELECT type, message, timestamp, is_read, username
                    FROM notifications 
                    ORDER BY timestamp DESC
                    LIMIT 100
                    """
                )
            else:
                # Regular users only see their own notifications
                self.cursor.execute(
                    """
                    SELECT type, message, timestamp, is_read, username
                    FROM notifications 
                    WHERE username = ? OR username = 'system'
                    ORDER BY timestamp DESC
                    LIMIT 100
                    """,
                    (st.session_state.username,)
                )
            
            notifications = self.cursor.fetchall()
            
            if not notifications:
                st.info("No notifications found.")
                return
            
            # Create DataFrame for display
            notifications_df = pd.DataFrame(
                notifications,
                columns=["Type", "Message", "Timestamp", "Read", "Username"]
            )
            
            # Format timestamp
            notifications_df["Timestamp"] = notifications_df["Timestamp"].apply(
                lambda x: datetime.fromtimestamp(x).strftime("%Y-%m-%d %H:%M:%S")
            )
            
            # Format read status
            notifications_df["Read"] = notifications_df["Read"].apply(
                lambda x: "✅" if x else "❌"
            )
            
            # Format notification type
            notifications_df["Type"] = notifications_df["Type"].apply(
                lambda x: {
                    "data_upload": "📊 Data Upload",
                    "new_table": "🆕 New Table",
                    "long_query": "⏱️ Long Query",
                    "database_reset": "🗑️ Database Reset",
                    "scheduled_upload": "🔄 Scheduled Upload"
                }.get(x, x)
            )
            
            # Display notifications
            st.dataframe(notifications_df)
            
            # Mark all as read button
            if st.button("Mark All as Read"):
                self.mark_all_notifications_read()
                st.success("All notifications marked as read.")
                st.rerun()
            
        except Exception as e:
            st.error(f"Error fetching notification history: {e}")
    
    def render_notification_settings(self):
        """Display and manage notification settings."""
        st.subheader("Notification Settings")
        
        # Get current settings
        settings = self.get_notification_settings()
        
        # Display settings form
        with st.form("notification_settings"):
            data_changes = st.checkbox("Notify on data changes", value=settings.get("data_changes", True))
            long_queries = st.checkbox("Notify on long-running queries", value=settings.get("long_queries", True))
            new_tables = st.checkbox("Notify when new tables are created", value=settings.get("new_tables", True))
            
            st.subheader("Email Notifications")
            email_alerts = st.checkbox("Send email alerts for important notifications", value=settings.get("email_alerts", False))
            email = st.text_input("Email address", value=settings.get("email", ""))
            
            if st.form_submit_button("Save Settings"):
                self.save_notification_settings(data_changes, long_queries, new_tables, email_alerts, email)
                st.success("Notification settings saved.")
    
    def get_notification_settings(self):
        """Get notification settings for the current user."""
        if not st.session_state.authenticated:
            return {}
        
        try:
            self.cursor.execute(
                """
                SELECT data_changes, long_queries, new_tables, email_alerts, email 
                FROM notification_settings 
                WHERE username = ?
                """,
                (st.session_state.username,)
            )
            
            settings = self.cursor.fetchone()
            
            if settings:
                return {
                    "data_changes": bool(settings[0]),
                    "long_queries": bool(settings[1]),
                    "new_tables": bool(settings[2]),
                    "email_alerts": bool(settings[3]),
                    "email": settings[4]
                }
            else:
                # Create default settings for new user
                self.cursor.execute(
                    """
                    INSERT INTO notification_settings (username, data_changes, long_queries, new_tables)
                    VALUES (?, 1, 1, 1)
                    """,
                    (st.session_state.username,)
                )
                self.conn.commit()
                
                return {
                    "data_changes": True,
                    "long_queries": True,
                    "new_tables": True,
                    "email_alerts": False,
                    "email": ""
                }
            
        except Exception as e:
            st.error(f"Error fetching notification settings: {e}")
            return {}
    
    def save_notification_settings(self, data_changes, long_queries, new_tables, email_alerts, email):
        """Save notification settings for the current user."""
        if not st.session_state.authenticated:
            return
        
        try:
            self.cursor.execute(
                """
                INSERT OR REPLACE INTO notification_settings 
                (username, data_changes, long_queries, new_tables, email_alerts, email)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (st.session_state.username, data_changes, long_queries, new_tables, email_alerts, email)
            )
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Error saving notification settings: {e}")
            return False
    
    def mark_all_notifications_read(self):
        """Mark all notifications as read for the current user, or all notifications for admin."""
        try:
            # Check if the user is admin
            is_admin = st.session_state.role == "admin"
            
            if is_admin:
                # Admin can mark all notifications as read
                self.cursor.execute(
                    "UPDATE notifications SET is_read = 1"
                )
            else:
                # Regular users can only mark their own notifications as read
                self.cursor.execute(
                    "UPDATE notifications SET is_read = 1 WHERE username = ? OR username = 'system'",
                    (st.session_state.username,)
                )
            
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Error marking notifications as read: {e}")
            return False
