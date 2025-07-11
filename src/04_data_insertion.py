import pandas as pd
import psycopg2
from psycopg2 import sql
import json
import sys
from datetime import datetime

# Configuration file paths
CONFIG_FILE = 'config/database_config.json'
CLEANED_DATA_FILE = 'data/students_cleaned.csv'

def load_database_config():
    """
    Load database configuration from the JSON file.
    This function ensures consistent configuration access across all scripts.
    """
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        return config['database']
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {CONFIG_FILE}")
        print("Please run the database setup script first (03_database_setup.py)")
        return None
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def create_database_connection(db_config):
    """
    Establish a connection to the student_analysis database.
    This function implements secure connection management with proper error handling.
    """
    try:
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
        print(f"❌ Unexpected connection error: {e}")
        return None

def load_cleaned_data():
    """
    Load the cleaned student data from CSV file and perform basic validation.
    This function bridges pandas data processing with database operations.
    """
    print("\n=== Loading Cleaned Student Data ===")
    
    try:
        df = pd.read_csv(CLEANED_DATA_FILE)
        print(f"✅ Successfully loaded {len(df)} records from {CLEANED_DATA_FILE}")
        
        # Display basic information about the dataset
        print(f"Dataset shape: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")
        
        # Verify that required columns exist
        required_columns = ['student name', 'first name', 'last name', 'age', 'average mark', 'gender', 'phone number']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return None
        
        print("✅ All required columns present")
        return df
        
    except FileNotFoundError:
        print(f"❌ Cleaned data file not found: {CLEANED_DATA_FILE}")
        print("Please run the data cleaning script first (02_data_cleaning.py)")
        return None
    except Exception as e:
        print(f"❌ Error loading cleaned data: {e}")
        return None

def validate_data_before_insertion(df):
    """
    Perform comprehensive data validation before attempting database insertion.
    This function catches potential issues early and provides detailed feedback.
    """
    print("\n=== Data Validation Before Insertion ===")
    
    validation_passed = True
    
    # Check for missing values in critical columns
    critical_columns = ['first name', 'last name', 'age', 'average mark', 'gender']
    for column in critical_columns:
        missing_count = df[column].isnull().sum()
        if missing_count > 0:
            print(f"❌ {column}: {missing_count} missing values")
            validation_passed = False
        else:
            print(f"✅ {column}: No missing values")
    
    # Validate data ranges and types
    if 'age' in df.columns:
        invalid_ages = df[(df['age'] < 1) | (df['age'] > 150)].shape[0]
        if invalid_ages > 0:
            print(f"❌ Found {invalid_ages} records with invalid age values")
            validation_passed = False
        else:
            print("✅ All age values within valid range")
    
    if 'average mark' in df.columns:
        invalid_marks = df[(df['average mark'] < 0) | (df['average mark'] > 10)].shape[0]
        if invalid_marks > 0:
            print(f"❌ Found {invalid_marks} records with invalid average mark values")
            validation_passed = False
        else:
            print("✅ All average mark values within valid range")
    
    if 'gender' in df.columns:
        valid_genders = df['gender'].isin(['m', 'f']).all()
        if not valid_genders:
            invalid_gender_count = (~df['gender'].isin(['m', 'f'])).sum()
            print(f"❌ Found {invalid_gender_count} records with invalid gender values")
            validation_passed = False
        else:
            print("✅ All gender values are valid")
    
    return validation_passed

def insert_student_data(conn, df, table_name):
    """
    Insert student data into the database using efficient batch processing.
    This function demonstrates professional bulk data insertion techniques.
    """
    print(f"\n=== Inserting Data into Table '{table_name}' ===")
    
    try:
        cursor = conn.cursor()
        
        # First, check if table already contains data
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            print(f"⚠️  Table already contains {existing_count} records")
            response = input("Do you want to proceed with insertion anyway? (y/n): ").lower().strip()
            if response != 'y':
                print("❌ Data insertion cancelled by user")
                return False
        
        # Prepare the SQL insertion statement
        insert_sql = f"""
        INSERT INTO {table_name} 
        (student_name, first_name, last_name, age, average_mark, gender, phone_number)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        # Prepare data for batch insertion
        insertion_data = []
        for index, row in df.iterrows():
            # Convert phone number to integer, handling any floating point representation
            phone_number = int(row['phone number']) if pd.notna(row['phone number']) else None
            
            record_data = (
                row['student name'],
                row['first name'],
                row['last name'],
                int(row['age']),
                float(row['average mark']),
                row['gender'],
                phone_number
            )
            insertion_data.append(record_data)
        
        # Perform batch insertion with transaction management
        print(f"Inserting {len(insertion_data)} records...")
        cursor.executemany(insert_sql, insertion_data)
        
        # Commit the transaction
        conn.commit()
        
        # Verify insertion success
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        
        print(f"✅ Successfully inserted {len(insertion_data)} records")
        print(f"✅ Total records in table: {final_count}")
        
        cursor.close()
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Database error during insertion: {e}")
        conn.rollback()  # Rollback transaction on error
        return False
    except Exception as e:
        print(f"❌ Unexpected error during insertion: {e}")
        conn.rollback()  # Rollback transaction on error
        return False

def verify_insertion_success(conn, table_name, expected_count):
    """
    Verify that data insertion completed successfully by performing validation queries.
    This function demonstrates how to validate database operations systematically.
    """
    print(f"\n=== Verifying Data Insertion Success ===")
    
    try:
        cursor = conn.cursor()
        
        # Check total record count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        actual_count = cursor.fetchone()[0]
        print(f"Expected records: {expected_count}")
        print(f"Actual records: {actual_count}")
        
        if actual_count >= expected_count:
            print("✅ Record count verification passed")
        else:
            print("❌ Record count verification failed")
            return False
        
        # Check for any null values in required fields
        cursor.execute(f"""
            SELECT 
                SUM(CASE WHEN first_name IS NULL THEN 1 ELSE 0 END) as null_first_names,
                SUM(CASE WHEN last_name IS NULL THEN 1 ELSE 0 END) as null_last_names,
                SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) as null_ages,
                SUM(CASE WHEN average_mark IS NULL THEN 1 ELSE 0 END) as null_marks,
                SUM(CASE WHEN gender IS NULL THEN 1 ELSE 0 END) as null_genders
            FROM {table_name}
        """)
        
        null_counts = cursor.fetchone()
        null_fields = ['first_name', 'last_name', 'age', 'average_mark', 'gender']
        
        all_checks_passed = True
        for i, field in enumerate(null_fields):
            if null_counts[i] == 0:
                print(f"✅ {field}: No null values")
            else:
                print(f"❌ {field}: {null_counts[i]} null values found")
                all_checks_passed = False
        
        # Display sample of inserted data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_records = cursor.fetchall()
        
        print("\nSample of inserted data:")
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
        """)
        column_names = [row[0] for row in cursor.fetchall()]
        
        for i, record in enumerate(sample_records, 1):
            print(f"  Record {i}: {dict(zip(column_names, record))}")
        
        cursor.close()
        return all_checks_passed
        
    except psycopg2.Error as e:
        print(f"❌ Database error during verification: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error during verification: {e}")
        return False

def main():
    """
    Main function that orchestrates the complete data insertion workflow.
    This demonstrates how to structure complex database operations systematically.
    """
    print("=== Student Data Insertion Pipeline ===")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Load configuration
    db_config = load_database_config()
    if db_config is None:
        print("❌ Cannot proceed without valid database configuration")
        sys.exit(1)
    
    # Step 2: Load cleaned data
    df = load_cleaned_data()
    if df is None:
        print("❌ Cannot proceed without valid cleaned data")
        sys.exit(1)
    
    # Step 3: Validate data before insertion
    if not validate_data_before_insertion(df):
        print("❌ Data validation failed - please fix data quality issues before proceeding")
        sys.exit(1)
    
    # Step 4: Connect to database
    conn = create_database_connection(db_config)
    if conn is None:
        print("❌ Cannot proceed without database connection")
        sys.exit(1)
    
    try:
        # Step 5: Insert data into database
        insertion_successful = insert_student_data(conn, df, db_config['table_name'])
        if not insertion_successful:
            print("❌ Data insertion failed")
            sys.exit(1)
        
        # Step 6: Verify insertion success
        verification_passed = verify_insertion_success(conn, db_config['table_name'], len(df))
        if verification_passed:
            print("\n=== Data Insertion Complete ===")
            print(f"✅ Successfully inserted and verified {len(df)} student records")
            print("✅ Database is ready for analytical queries")
        else:
            print("\n❌ Data insertion completed but verification failed")
            print("Please check the database manually for any issues")
    
    finally:
        # Always close database connection
        conn.close()
        print("Database connection closed")
        print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()