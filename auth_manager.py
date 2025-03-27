import streamlit as st
import streamlit_authenticator as stauth
import sqlite3
import hashlib
import time
import bcrypt
import json
import os
from datetime import datetime

class AuthManager:
    """Manages user authentication and session handling."""
    
    def __init__(self, sqlite_conn):
        self.conn = sqlite_conn
        self.cursor = self.conn.cursor()
        self.setup_user_tables()
        
        # Create default admin user if not exists
        self.create_default_admin()
    
    def setup_user_tables(self):
        """Create tables for user authentication and session management."""
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            email TEXT UNIQUE,
            role TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        ''')
        
        self.cursor.execute('''
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            session_id TEXT UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            expires_at TIMESTAMP NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            FOREIGN KEY (username) REFERENCES users(username)
        )
        ''')
        
        self.conn.commit()
    
    def create_default_admin(self):
        """Create a default admin user if no users exist."""
        self.cursor.execute("SELECT COUNT(*) FROM users")
        user_count = self.cursor.fetchone()[0]
        
        if user_count == 0:
            # Generate a hashed password for "admin"
            hashed_password = bcrypt.hashpw("admin".encode(), bcrypt.gensalt()).decode()
            
            # Insert default admin user
            self.cursor.execute(
                "INSERT INTO users (username, password, email, role) VALUES (?, ?, ?, ?)",
                ("admin", hashed_password, "admin@example.com", "admin")
            )
            
            # Insert default viewer user
            hashed_password = bcrypt.hashpw("viewer".encode(), bcrypt.gensalt()).decode()
            self.cursor.execute(
                "INSERT INTO users (username, password, email, role) VALUES (?, ?, ?, ?)",
                ("viewer", hashed_password, "viewer@example.com", "viewer")
            )
            
            self.conn.commit()
            st.info("Default users created: admin/admin and viewer/viewer")
    
    def authenticate_user(self):
        """Display login form and authenticate user."""
        st.subheader("Login")
        
        # Create login form
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        
        if st.button("Login"):
            if username and password:
                # Check if user exists
                self.cursor.execute("SELECT username, password, role FROM users WHERE username = ?", (username,))
                user_record = self.cursor.fetchone()
                
                if user_record and bcrypt.checkpw(password.encode(), user_record[1].encode()):
                    # Authentication successful
                    self.create_session(username)
                    return True, username, user_record[2]
                else:
                    return False, None, None
            else:
                st.warning("Please enter both username and password.")
                return None, None, None
        
        # Registration option
        st.divider()
        if st.checkbox("New user? Register here"):
            self.show_registration_form()
        
        return None, None, None
    
    def show_registration_form(self):
        """Display registration form for new users."""
        st.subheader("Register")
        
        reg_username = st.text_input("Choose Username", key="reg_username")
        reg_password = st.text_input("Choose Password", type="password", key="reg_password")
        reg_password_confirm = st.text_input("Confirm Password", type="password", key="reg_password_confirm")
        reg_email = st.text_input("Email", key="reg_email")
        
        if st.button("Register"):
            if not reg_username or not reg_password or not reg_email:
                st.warning("All fields are required.")
                return
            
            if reg_password != reg_password_confirm:
                st.warning("Passwords do not match.")
                return
            
            # Check if username already exists
            self.cursor.execute("SELECT username FROM users WHERE username = ?", (reg_username,))
            if self.cursor.fetchone():
                st.warning("Username already exists. Please choose another.")
                return
            
            # Check if email already exists
            self.cursor.execute("SELECT email FROM users WHERE email = ?", (reg_email,))
            if self.cursor.fetchone():
                st.warning("Email already exists. Please use another.")
                return
            
            # Hash password
            hashed_password = bcrypt.hashpw(reg_password.encode(), bcrypt.gensalt()).decode()
            
            # Insert new user (default role is viewer)
            self.cursor.execute(
                "INSERT INTO users (username, password, email, role) VALUES (?, ?, ?, ?)",
                (reg_username, hashed_password, reg_email, "viewer")
            )
            self.conn.commit()
            
            st.success("Registration successful! You can now login.")
    
    def create_session(self, username):
        """Create a new session for the authenticated user."""
        session_id = hashlib.sha256(f"{username}:{time.time()}".encode()).hexdigest()
        expires_at = datetime.now().timestamp() + (24 * 60 * 60)  # 24 hours from now
        
        self.cursor.execute(
            "INSERT INTO sessions (username, session_id, expires_at) VALUES (?, ?, ?)",
            (username, session_id, expires_at)
        )
        self.conn.commit()
        
        # Store session in cookie or session state
        st.session_state.session_id = session_id
        return session_id
    
    def validate_session(self, session_id):
        """Validate if a session is active and not expired."""
        if not session_id:
            return False
        
        self.cursor.execute(
            "SELECT username, expires_at FROM sessions WHERE session_id = ? AND is_active = 1",
            (session_id,)
        )
        session = self.cursor.fetchone()
        
        if not session:
            return False
        
        username, expires_at = session
        
        # Check if session has expired
        if time.time() > expires_at:
            self.invalidate_session(session_id)
            return False
        
        return username
    
    def invalidate_session(self, session_id):
        """Invalidate a user session."""
        self.cursor.execute(
            "UPDATE sessions SET is_active = 0 WHERE session_id = ?",
            (session_id,)
        )
        self.conn.commit()
    
    def logout(self):
        """Log out the current user by invalidating their session."""
        if 'session_id' in st.session_state:
            self.invalidate_session(st.session_state.session_id)
            del st.session_state.session_id
            
        # Clear all session state variables
        for key in ['authenticated', 'username', 'role', 'current_db', 'query_history', 'saved_queries']:
            if key in st.session_state:
                del st.session_state[key]
        
        return True
    
    def get_user_role(self, username):
        """Get the role of a specific user."""
        self.cursor.execute("SELECT role FROM users WHERE username = ?", (username,))
        result = self.cursor.fetchone()
        
        if result:
            return result[0]
        return None
