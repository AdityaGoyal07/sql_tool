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
        
        if st.button("Save Notification Settings"):
            # In a real application, these would be saved in the database
            st.success("Notification settings saved successfully!")
