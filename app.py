import streamlit as st
import time

# Import all the managers
from db_manager import DatabaseManager
from auth_manager import AuthManager
from query_manager import QueryManager
from upload_manager import UploadManager
from visualization_manager import VisualizationManager
from notification_manager import NotificationManager
from scheduler_manager import SchedulerManager
from background_processor import BackgroundProcessor
from query_builder import QueryBuilder
from ai_assistant import AIAssistant
from utils import create_sqlite_connection

# Configure the page
st.set_page_config(
    page_title="Advanced SQL Query GUI",
    page_icon="🛢️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state variables if they don't exist
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'role' not in st.session_state:
    st.session_state.role = None
if 'current_db' not in st.session_state:
    st.session_state.current_db = 'postgresql'  # Default database
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'saved_queries' not in st.session_state:
    st.session_state.saved_queries = []
if 'notification_queue' not in st.session_state:
    st.session_state.notification_queue = []
if 'last_notification_check' not in st.session_state:
    st.session_state.last_notification_check = time.time()
if 'current_query_time' not in st.session_state:
    st.session_state.current_query_time = None
if 'last_query_result' not in st.session_state:
    st.session_state.last_query_result = None

# Create SQLite connection for storing user data, query history, etc.
sqlite_conn = create_sqlite_connection()

# Initialize auth manager
auth_manager = AuthManager(sqlite_conn)

# Handle authentication
if not st.session_state.authenticated:
    auth_status, username, role = auth_manager.authenticate_user()
    if auth_status:
        st.session_state.authenticated = True
        st.session_state.username = username
        st.session_state.role = role
        st.rerun()
    elif auth_status == False:
        st.error('Username/password is incorrect')
    # Don't show the rest of the app when not authenticated
    if not st.session_state.authenticated:
        st.stop()

# Initialize all the managers
db_manager = DatabaseManager()
query_manager = QueryManager(sqlite_conn)
upload_manager = UploadManager(sqlite_conn)
visualization_manager = VisualizationManager()
notification_manager = NotificationManager(sqlite_conn)
scheduler_manager = SchedulerManager(sqlite_conn)
background_processor = BackgroundProcessor()
query_builder = QueryBuilder()
ai_assistant = AIAssistant()

# Main app
def main():
    st.title("Advanced SQL Query GUI")
    
    with st.sidebar:
        st.header("User: " + st.session_state.username)
        st.text("Role: " + st.session_state.role)
        st.divider()
        
        # Database connection selection
        st.header("Database Connection")
        db_type = st.selectbox("Select Database Type", 
                             ["MySQL", "PostgreSQL", "SQLite"], 
                             index=1)  # Default to PostgreSQL
        
        if db_type == "MySQL":
            db_connection = db_manager.connect_to_mysql_db()
        elif db_type == "PostgreSQL":
            db_connection = db_manager.connect_to_postgres_db()
        elif db_type == "SQLite":
            db_path = st.text_input("SQLite Database Path", "sqlite.db")
            db_connection = db_manager.connect_to_sqlite_db(db_path)
        
        st.divider()
        
        # App navigation
        st.header("Navigation")
        app_mode = st.radio("Select Mode", 
                          ["Upload Data", "Query Builder", "SQL Query", 
                           "AI Assistant", "Query History", 
                           "Scheduled Uploads", "Notifications"])
        
        # Admin-only options
        if st.session_state.role == "admin":
            st.divider()
            st.header("Admin Actions")
            if st.button("🗑️ Reset Database"):
                db_manager.reset_database(db_connection)
                st.success("Database reset completed!")
                time.sleep(1)
                st.rerun()
        
        # Logout button
        st.divider()
        if st.button("Logout"):
            auth_manager.logout()
            st.session_state.authenticated = False
            st.rerun()
    
    # Check for notifications
    notification_manager.check_notifications()
    
    # Main content based on selected mode
    if app_mode == "Upload Data":
        upload_manager.render_upload_interface(db_connection)
    
    elif app_mode == "Query Builder":
        query_builder.render_query_builder(db_connection)
    
    elif app_mode == "SQL Query":
        query_manager.render_query_interface(db_connection)
    
    elif app_mode == "AI Assistant":
        ai_assistant.render_ai_interface(db_connection)
    
    elif app_mode == "Query History":
        query_manager.render_history_interface()
    
    elif app_mode == "Scheduled Uploads":
        scheduler_manager.render_scheduler_interface()
    
    elif app_mode == "Notifications":
        notification_manager.render_notification_interface()

if __name__ == "__main__":
    main()
