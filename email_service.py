import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import streamlit as st

class EmailService:
    """Handles sending email notifications for background tasks and alerts."""
    
    def __init__(self):
        # Get email configuration from environment variables
        self.smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
        self.smtp_port = int(os.getenv("SMTP_PORT", "587"))
        self.sender_email = os.getenv("SENDER_EMAIL", "")
        self.sender_password = os.getenv("SENDER_PASSWORD", "")
    
    def send_email(self, recipient_email, subject, body, is_html=True):
        """Send an email notification."""
        # Skip sending if email credentials are not configured
        if not self.sender_email or not self.sender_password:
            print("Email credentials not configured. Email not sent.")
            return False
        
        try:
            # Create message
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = self.sender_email
            msg["To"] = recipient_email
            
            # Attach the body
            if is_html:
                msg.attach(MIMEText(body, "html"))
            else:
                msg.attach(MIMEText(body, "plain"))
            
            # Send the email
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, recipient_email, msg.as_string())
            
            return True
        
        except Exception as e:
            print(f"Error sending email: {e}")
            return False
    
    def send_query_completion_notification(self, recipient_email, task_id, execution_time, row_count):
        """Send an email notification for a completed query task."""
        subject = f"SQL Query GUI - Query Task #{task_id} Completed"
        
        body = f"""
        <html>
        <body>
            <h2>Query Task Completed</h2>
            <p>Your SQL query task has been completed successfully.</p>
            <ul>
                <li><strong>Task ID:</strong> {task_id}</li>
                <li><strong>Execution Time:</strong> {execution_time:.2f} seconds</li>
                <li><strong>Results:</strong> {row_count} rows</li>
            </ul>
            <p>You can view the complete results in the SQL Query GUI application.</p>
        </body>
        </html>
        """
        
        return self.send_email(recipient_email, subject, body)
    
    def send_query_failure_notification(self, recipient_email, task_id, error_message):
        """Send an email notification for a failed query task."""
        subject = f"SQL Query GUI - Query Task #{task_id} Failed"
        
        body = f"""
        <html>
        <body>
            <h2>Query Task Failed</h2>
            <p>Your SQL query task has failed to complete.</p>
            <ul>
                <li><strong>Task ID:</strong> {task_id}</li>
                <li><strong>Error:</strong> {error_message}</li>
            </ul>
            <p>Please check your query and try again.</p>
        </body>
        </html>
        """
        
        return self.send_email(recipient_email, subject, body)
    
    def send_scheduled_upload_notification(self, recipient_email, table_name, source_type, row_count):
        """Send an email notification for a completed scheduled upload."""
        subject = f"SQL Query GUI - Scheduled Upload Completed"
        
        body = f"""
        <html>
        <body>
            <h2>Scheduled Upload Completed</h2>
            <p>Your scheduled data upload has been completed successfully.</p>
            <ul>
                <li><strong>Table:</strong> {table_name}</li>
                <li><strong>Source:</strong> {source_type}</li>
                <li><strong>Rows Uploaded:</strong> {row_count}</li>
            </ul>
            <p>You can view the data in the SQL Query GUI application.</p>
        </body>
        </html>
        """
        
        return self.send_email(recipient_email, subject, body)
    
    def send_scheduled_upload_failure_notification(self, recipient_email, table_name, source_type, error_message):
        """Send an email notification for a failed scheduled upload."""
        subject = f"SQL Query GUI - Scheduled Upload Failed"
        
        body = f"""
        <html>
        <body>
            <h2>Scheduled Upload Failed</h2>
            <p>Your scheduled data upload has failed to complete.</p>
            <ul>
                <li><strong>Table:</strong> {table_name}</li>
                <li><strong>Source:</strong> {source_type}</li>
                <li><strong>Error:</strong> {error_message}</li>
            </ul>
            <p>Please check the upload configuration and try again.</p>
        </body>
        </html>
        """
        
        return self.send_email(recipient_email, subject, body)
    
    def send_test_email(self, recipient_email):
        """Send a test email to verify email configuration."""
        subject = "SQL Query GUI - Email Test"
        
        body = """
        <html>
        <body>
            <h2>Email Configuration Test</h2>
            <p>This is a test email from the SQL Query GUI application.</p>
            <p>If you receive this email, it means your email configuration is working correctly.</p>
        </body>
        </html>
        """
        
        success = self.send_email(recipient_email, subject, body)
        return success
        
    def send_admin_log_report(self, admin_email, log_entries, report_type="daily"):
        """Send a log report email to admin with important system events.
        
        Args:
            admin_email: The admin's email address
            log_entries: List of log entries (dict with type, message, timestamp, username)
            report_type: Type of report ("daily", "weekly", or "custom")
        """
        subject = f"SQL Query GUI - Admin {report_type.capitalize()} Log Report"
        
        # Group logs by type
        log_types = {}
        for entry in log_entries:
            log_type = entry.get("type", "general")
            if log_type not in log_types:
                log_types[log_type] = []
            log_types[log_type].append(entry)
        
        # Generate HTML content
        body = f"""
        <html>
        <body>
            <h2>Admin {report_type.capitalize()} Log Report</h2>
            <p>This report contains important system events that may require your attention.</p>
        """
        
        # Add log sections by type
        for log_type, entries in log_types.items():
            formatted_type = log_type.replace("_", " ").title()
            body += f"""
            <h3>{formatted_type} Logs ({len(entries)})</h3>
            <table border="1" cellpadding="5" style="border-collapse: collapse; width: 100%;">
                <tr style="background-color: #f2f2f2;">
                    <th>Timestamp</th>
                    <th>User</th>
                    <th>Message</th>
                </tr>
            """
            
            for entry in entries:
                timestamp = entry.get("timestamp", "")
                username = entry.get("username", "system")
                message = entry.get("message", "")
                
                body += f"""
                <tr>
                    <td>{timestamp}</td>
                    <td>{username}</td>
                    <td>{message}</td>
                </tr>
                """
            
            body += "</table><br>"
        
        body += """
            <p>This is an automated report. Please do not reply to this email.</p>
        </body>
        </html>
        """
        
        return self.send_email(admin_email, subject, body)
    
    def render_email_configuration_interface(self):
        """Display the email configuration interface."""
        st.header("Email Configuration")
        
        # Check if user has admin role
        if st.session_state.role != "admin":
            st.warning("Email configuration requires admin permissions.")
            return
        
        # Display current configuration
        st.subheader("Current Configuration")
        
        smtp_server = st.text_input("SMTP Server:", value=self.smtp_server)
        smtp_port = st.number_input("SMTP Port:", value=self.smtp_port)
        sender_email = st.text_input("Sender Email:", value=self.sender_email)
        sender_password = st.text_input("Sender Password:", value=self.sender_password, type="password")
        
        if st.button("Save Configuration"):
            # In a real application, these would be saved securely
            # For this demo, we'll just update the instance variables
            self.smtp_server = smtp_server
            self.smtp_port = smtp_port
            self.sender_email = sender_email
            self.sender_password = sender_password
            
            st.success("Email configuration saved successfully!")
        
        # Test email configuration
        st.subheader("Test Email Configuration")
        
        test_email = st.text_input("Test Recipient Email:")
        
        if st.button("Send Test Email"):
            if test_email:
                with st.spinner("Sending test email..."):
                    success = self.send_test_email(test_email)
                    
                    if success:
                        st.success(f"Test email sent successfully to {test_email}!")
                    else:
                        st.error("Failed to send test email. Please check your configuration.")
            else:
                st.warning("Please enter a recipient email address.")
        
        # Email notification settings
        st.subheader("Notification Settings")
        
        background_tasks = st.checkbox("Send email notifications for background tasks", value=True)
        scheduled_uploads = st.checkbox("Send email notifications for scheduled uploads", value=True)
        query_errors = st.checkbox("Send email notifications for query errors", value=True)
        
        # Admin notification options (only visible to admins)
        if st.session_state.role == "admin":
            st.subheader("Admin Notifications")
            admin_email = st.text_input("Admin Email for Reports:", value=self.sender_email)
            
            st.write("Log Report Schedule")
            report_frequency = st.selectbox(
                "Send admin log reports:", 
                ["Never", "Daily", "Weekly", "Monthly"],
                index=0
            )
            
            include_user_actions = st.checkbox("Include user actions in admin reports", value=True)
            include_system_events = st.checkbox("Include system events in admin reports", value=True)
            include_errors = st.checkbox("Include errors in admin reports", value=True)
            
            # Option to generate a test admin report
            if st.button("Send Test Admin Report"):
                if admin_email:
                    with st.spinner("Generating and sending admin report..."):
                        # Generate sample log entries for the test report
                        sample_logs = [
                            {"type": "data_upload", "message": "Table sample_data was created with 1000 rows by user1", 
                             "timestamp": "2025-03-27 08:15:23", "username": "user1"},
                            {"type": "system_event", "message": "Database backup completed successfully", 
                             "timestamp": "2025-03-27 09:30:45", "username": "system"},
                            {"type": "error", "message": "Query execution failed: syntax error at line 3", 
                             "timestamp": "2025-03-27 10:45:12", "username": "user2"},
                            {"type": "user_action", "message": "User requested scheduled upload approval", 
                             "timestamp": "2025-03-27 11:20:33", "username": "user3"}
                        ]
                        
                        success = self.send_admin_log_report(admin_email, sample_logs, "test")
                        
                        if success:
                            st.success(f"Test admin report sent successfully to {admin_email}!")
                        else:
                            st.error("Failed to send test admin report. Please check your configuration.")
                else:
                    st.warning("Please enter an admin email address.")
        
        if st.button("Save Notification Settings"):
            # In a real application, these would be saved in the database
            st.success("Notification settings saved successfully!")
