import pandas as pd
import psycopg2
import json
from datetime import datetime

# Configuration file path
CONFIG_FILE = 'config/database_config.json'

def load_database_config():
    """
    Load database configuration from the JSON file.
    This ensures consistent access to our secure database credentials.
    """
    try:
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        return config['database']
    except FileNotFoundError:
        print(f"❌ Configuration file not found: {CONFIG_FILE}")
        print("Please ensure the database setup script has been run successfully")
        return None
    except Exception as e:
        print(f"❌ Error loading configuration: {e}")
        return None

def create_database_connection(db_config):
    """
    Establish a secure connection to our student_analysis database.
    This function applies the same connection management techniques we've mastered.
    """
    try:
        # Extract only connection parameters, excluding table configuration
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

def analyze_student_performance_by_gender(conn, table_name):
    """
    Perform the core analytical query: count students by gender with average marks > 5.0
    This function demonstrates how SQL can express complex analytical questions concisely.
    """
    print("\n=== Analyzing Student Performance by Gender ===")
    
    try:
        cursor = conn.cursor()
        
        # First, let's understand our complete dataset structure
        print("Dataset Overview:")
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_students,
                COUNT(DISTINCT gender) as gender_categories,
                MIN(average_mark) as min_average_mark,
                MAX(average_mark) as max_average_mark,
                AVG(average_mark) as overall_average_mark
            FROM {table_name}
        """)
        
        overview = cursor.fetchone()
        print(f"  Total students in database: {overview[0]}")
        print(f"  Gender categories: {overview[1]}")
        print(f"  Average mark range: {overview[2]:.2f} to {overview[3]:.2f}")
        print(f"  Overall average mark: {overview[4]:.2f}")
        
        # Now perform the specific analysis required by the assignment
        print(f"\nStudents with average marks > 5.0:")
        
        # This query does the following:
        # 1. Filtering with WHERE clause (average_mark > 5.0)
        # 2. Grouping by categorical variable (gender)
        # 3. Counting records within each group
        # 4. Ordering results for consistent presentation
        analysis_query = f"""
            SELECT 
                gender,
                COUNT(*) as student_count,
                AVG(average_mark) as avg_mark_in_group,
                MIN(average_mark) as min_mark_in_group,
                MAX(average_mark) as max_mark_in_group
            FROM {table_name} 
            WHERE average_mark > 5.0 
            GROUP BY gender 
            ORDER BY gender
        """
        
        cursor.execute(analysis_query)
        results = cursor.fetchall()
        
        # Display results in a clear, professional format
        print(f"\nQuery Results:")
        for row in results:
            gender_label = "Male" if row[0] == 'm' else "Female"
            print(f"  {gender_label} students: {row[1]} students")
            print(f"    Average mark in group: {row[2]:.2f}")
            print(f"    Mark range in group: {row[3]:.2f} to {row[4]:.2f}")
        
        cursor.close()
        return results
        
    except psycopg2.Error as e:
        print(f"❌ Database error during analysis: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error during analysis: {e}")
        return None

def create_pandas_dataframe_from_results(results):
    """
    Convert our SQL query results into a pandas DataFrame as required by the assignment.
    This demonstrates how to bridge between database queries and pandas analysis tools.
    """
    print("\n=== Creating pandas DataFrame from Query Results ===")
    
    if not results:
        print("❌ No results available to convert to DataFrame")
        return None
    
    # Transform the SQL results into a structure suitable for pandas
    # We'll create descriptive column names and readable gender labels
    data_for_dataframe = []
    for row in results:
        gender_label = "Male" if row[0] == 'm' else "Female"
        data_for_dataframe.append({
            'Gender': gender_label,
            'Student_Count': row[1],
            'Average_Mark': round(row[2], 2),
            'Min_Mark': round(row[3], 2),
            'Max_Mark': round(row[4], 2)
        })
    
    # Create the pandas DataFrame
    df_results = pd.DataFrame(data_for_dataframe)
    
    print("✅ Successfully created pandas DataFrame from query results")
    print("\nDataFrame Contents:")
    print(df_results.to_string(index=False))
    
    # Calculate some additional insights using pandas capabilities
    print(f"\nAdditional Analysis Using pandas:")
    total_high_performers = df_results['Student_Count'].sum()
    print(f"Total students with average marks > 5.0: {total_high_performers}")
    
    if len(df_results) > 1:
        gender_with_most = df_results.loc[df_results['Student_Count'].idxmax(), 'Gender']
        highest_count = df_results['Student_Count'].max()
        print(f"Gender with most high performers: {gender_with_most} ({highest_count} students)")
        
        # Calculate percentage distribution
        df_results['Percentage'] = (df_results['Student_Count'] / total_high_performers * 100).round(1)
        print(f"\nPercentage Distribution:")
        for _, row in df_results.iterrows():
            print(f"  {row['Gender']}: {row['Percentage']}% of high performers")
    
    return df_results

def perform_additional_exploratory_analysis(conn, table_name):
    """
    Demonstrate additional analytical capabilities that our database infrastructure supports.
    This shows how proper database design enables flexible analytical exploration.
    """
    print("\n=== Additional Exploratory Analysis ===")
    
    try:
        cursor = conn.cursor()
        
        # Analyze performance distribution across different criteria
        print("Performance Distribution Analysis:")
        
        # Age group analysis
        cursor.execute(f"""
            SELECT 
                CASE 
                    WHEN age < 20 THEN 'Under 20'
                    WHEN age BETWEEN 20 AND 25 THEN '20-25'
                    ELSE 'Over 25'
                END as age_group,
                COUNT(*) as total_students,
                COUNT(CASE WHEN average_mark > 5.0 THEN 1 END) as high_performers,
                AVG(average_mark) as avg_mark
            FROM {table_name}
            GROUP BY 
                CASE 
                    WHEN age < 20 THEN 'Under 20'
                    WHEN age BETWEEN 20 AND 25 THEN '20-25'
                    ELSE 'Over 25'
                END
            ORDER BY avg_mark DESC
        """)
        
        age_results = cursor.fetchall()
        print(f"\nPerformance by Age Group:")
        for row in age_results:
            performance_rate = (row[2] / row[1] * 100) if row[1] > 0 else 0
            print(f"  {row[0]}: {row[2]}/{row[1]} high performers ({performance_rate:.1f}%), avg mark: {row[3]:.2f}")
        
        # Overall performance statistics
        cursor.execute(f"""
            SELECT 
                COUNT(*) as total_students,
                COUNT(CASE WHEN average_mark > 5.0 THEN 1 END) as above_threshold,
                COUNT(CASE WHEN average_mark <= 5.0 THEN 1 END) as at_or_below_threshold,
                AVG(average_mark) as overall_average
            FROM {table_name}
        """)
        
        stats = cursor.fetchone()
        success_rate = (stats[1] / stats[0] * 100) if stats[0] > 0 else 0
        print(f"\nOverall Performance Statistics:")
        print(f"  Total students: {stats[0]}")
        print(f"  Students with marks > 5.0: {stats[1]} ({success_rate:.1f}%)")
        print(f"  Students with marks ≤ 5.0: {stats[2]} ({100-success_rate:.1f}%)")
        print(f"  Overall average mark: {stats[3]:.2f}")
        
        cursor.close()
        
    except psycopg2.Error as e:
        print(f"❌ Database error during exploratory analysis: {e}")
    except Exception as e:
        print(f"❌ Unexpected error during exploratory analysis: {e}")

def main():
    """
    Main function that orchestrates the complete analytical workflow.
    This demonstrates how professional data analysis projects structure their operations.
    """
    print("=== Student Performance Analysis Pipeline ===")
    print(f"Analysis started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Load configuration and establish database connection
    db_config = load_database_config()
    if db_config is None:
        print("❌ Cannot proceed without valid database configuration")
        return
    
    conn = create_database_connection(db_config)
    if conn is None:
        print("❌ Cannot proceed without database connection")
        return
    
    try:
        # Step 2: Perform the core analysis required by the assignment
        results = analyze_student_performance_by_gender(conn, db_config['table_name'])
        if results is None:
            print("❌ Core analysis failed")
            return
        
        # Step 3: Convert results to pandas DataFrame as specified in assignment
        df_results = create_pandas_dataframe_from_results(results)
        if df_results is None:
            print("❌ DataFrame creation failed")
            return
        
        # Step 4: Demonstrate additional analytical capabilities
        perform_additional_exploratory_analysis(conn, db_config['table_name'])
        
        # Step 5: Summarize key findings
        print(f"\n=== Analysis Summary ===")
        print(f"✅ Successfully analyzed student performance patterns")
        print(f"✅ Created pandas DataFrame with {len(df_results)} gender groups")
        print(f"✅ Demonstrated database query optimization and pandas integration")
        print(f"✅ Assignment requirements completed successfully")
        
    finally:
        # Always ensure clean resource management
        conn.close()
        print(f"\nDatabase connection closed")
        print(f"Analysis completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()