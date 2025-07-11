import pandas as pd
import psycopg2
from psycopg2 import sql
import json
import os

# Configuration file path
CONFIG_FILE = 'config/database_config.json'
CLEANED_DATA_FILE = 'data/students_cleaned.csv'

def create_database_config():
    """
    Create a configuration file to store database connection parameters.
    This demonstrates how to separate sensitive configuration from code logic.
    """
    print("=== Creating Database Configuration ===")
    
    # Ensure config directory exists
    os.makedirs('config', exist_ok=True)
    
    # Database configuration - modify these values to match your PostgreSQL setup
    config = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "database": "student_analysis",
            "user": "student_analyst",  # Replace with your PostgreSQL username
            "password": "assignment_2025",  # Replace with your PostgreSQL password
            "table_name": "students"
        }
    }
    
    try:
        with open(CONFIG_FILE, 'w') as f:
            json.dump(config, f, indent=4)
        print(f"✅ Configuration file created at {CONFIG_FILE}")
        print("⚠️  Please update the database credentials in the config file before proceeding")
        return config
    except Exception as e:
        print(f"❌ Error creating config file: {e}")
        return None

def load_database_config():
    """
    Load database configuration from the JSON file.
    This function demonstrates secure configuration management practices.
    """
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        return config['database']
    except FileNotFoundError:
        print(f"❌ Configuration file not found at {CONFIG_FILE}")
        print("Creating a template configuration file...")
        return create_database_config()
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def create_database_connection(db_config):
    """
    Establish a connection to the PostgreSQL database.
    This function demonstrates proper database connection management.
    """
    try:
        # First, connect to PostgreSQL server (without specifying database)
        # to create the database if it doesn't exist
        server_config = {
            'host': db_config['host'],
            'port': db_config['port'],
            'database': 'postgres',  # Default database to connect for creation
            'user': db_config['user'],
            'password': db_config['password']
        }
        
        conn = psycopg2.connect(**server_config)
        conn.autocommit = True  # Required for CREATE DATABASE operations
        
        cursor = conn.cursor()
        
        # Check if our target database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_config['database'],))
        exists = cursor.fetchone()
        
        if not exists:
            print(f"Creating database '{db_config['database']}'...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(
                sql.Identifier(db_config['database'])
            ))
            print(f"✅ Database '{db_config['database']}' created successfully")
        else:
            print(f"✅ Database '{db_config['database']}' already exists")
        
        cursor.close()
        conn.close()
        
        # Now connect to the target database
        # Extract only connection parameters, excluding table_name
        connection_params = {
            'host': db_config['host'],
            'port': db_config['port'],
            'database': db_config['database'],
            'user': db_config['user'],
            'password': db_config['password']
        }
        conn = psycopg2.connect(**connection_params)
        print(f"✅ Connected to database '{db_config['database']}'")
        return conn
        
    except psycopg2.Error as e:
        print(f"❌ Database connection error: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def create_students_table(conn, table_name):
    """
    Create the students table with appropriate schema.
    This function demonstrates SQL DDL (Data Definition Language) operations.
    """
    print(f"\n=== Creating Table '{table_name}' ===")
    
    # SQL statement to create the students table
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        student_name VARCHAR(100) NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        age INTEGER NOT NULL CHECK (age > 0),
        average_mark NUMERIC(4,2) NOT NULL CHECK (average_mark >= 0 AND average_mark <= 10),
        gender CHAR(1) NOT NULL CHECK (gender IN ('m', 'f')),
        phone_number BIGINT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        
        print(f"✅ Table '{table_name}' created successfully")
        
        # Display table structure for verification
        cursor.execute(f"""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position;
        """)
        
        columns = cursor.fetchall()
        print("\nTable structure:")
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            default = f" DEFAULT {col[3]}" if col[3] else ""
            print(f"  {col[0]}: {col[1]} {nullable}{default}")
        
        cursor.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Error creating table: {e}")
        conn.rollback()
        return False

def load_cleaned_data():
    """
    Load the cleaned student data from CSV file.
    This function bridges our pandas data processing with database operations.
    """
    print(f"\n=== Loading Cleaned Data ===")
    
    try:
        df = pd.read_csv(CLEANED_DATA_FILE)
        print(f"✅ Loaded {len(df)} records from {CLEANED_DATA_FILE}")
        print(f"Columns: {list(df.columns)}")
        return df
    except FileNotFoundError:
        print(f"❌ Cleaned data file not found: {CLEANED_DATA_FILE}")
        print("Please run the data cleaning script first (02_data_cleaning.py)")
        return None
    except Exception as e:
        print(f"❌ Error loading cleaned data: {e}")
        return None

def main():
    """
    Main function that orchestrates the complete database setup workflow.
    This demonstrates how to structure complex database operations.
    """
    print("=== Student Database Setup Pipeline ===")
    
    # Step 1: Load configuration
    db_config = load_database_config()
    if db_config is None:
        print("❌ Cannot proceed without valid database configuration")
        return
    
    # Step 2: Create database connection
    conn = create_database_connection(db_config)
    if conn is None:
        print("❌ Cannot proceed without database connection")
        return
    
    try:
        # Step 3: Create students table
        table_created = create_students_table(conn, db_config['table_name'])
        if not table_created:
            print("❌ Cannot proceed without valid table structure")
            return
        
        # Step 4: Load cleaned data
        df = load_cleaned_data()
        if df is None:
            print("❌ Cannot proceed without cleaned data")
            return
        
        print("\n=== Database Setup Complete ===")
        print(f"Ready for data insertion: {len(df)} records prepared")
        print("Next: Run data insertion script to populate the database")
        
    finally:
        # Always close database connection
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    main()