import pandas as pd
import os

# Configuration
DATA_FILE = 'data/students.xlsx'
OUTPUT_FILE = 'data/students_cleaned.csv'

def load_and_validate_data():
    """
    Load the original dataset and perform basic validation.
    This function ensures we're working with the expected data structure.
    """
    print("=== Loading Original Dataset ===")
    
    try:
        df = pd.read_excel(DATA_FILE)
        print(f"✅ Successfully loaded {len(df)} records from {DATA_FILE}")
        print(f"Dataset shape: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"Columns: {list(df.columns)}")
        return df
    except FileNotFoundError:
        print(f"❌ Error: Could not find {DATA_FILE}")
        return None
    except Exception as e:
        print(f"❌ Error loading data: {e}")
        return None

def remove_missing_average_marks(df):
    """
    Remove records where 'average mark' is missing or null.
    This implements one of the core requirements from the assignment.
    """
    print("\n=== Removing Records with Missing Average Marks ===")
    
    # Count missing values before cleaning
    initial_count = len(df)
    missing_marks = df['average mark'].isnull().sum()
    
    print(f"Records before cleaning: {initial_count}")
    print(f"Records with missing average marks: {missing_marks}")
    
    # Remove rows where average mark is missing
    df_cleaned = df.dropna(subset=['average mark']).copy()
    
    final_count = len(df_cleaned)
    removed_count = initial_count - final_count
    
    print(f"Records after cleaning: {final_count}")
    print(f"Records removed: {removed_count}")
    
    return df_cleaned

def split_student_names(df):
    """
    Split 'student name' column into separate 'first name' and 'last name' columns.
    This demonstrates string manipulation and DataFrame column operations.
    """
    print("\n=== Splitting Student Names ===")
    
    # Split names and create new columns
    name_parts = df['student name'].str.split(' ', expand=True)
    
    # Add new columns to the DataFrame
    df['first name'] = name_parts[0]
    df['last name'] = name_parts[1]
    
    # Display some examples to verify the splitting worked correctly
    print("Sample name splitting results:")
    sample_names = df[['student name', 'first name', 'last name']].head()
    print(sample_names.to_string(index=False))
    
    # Check for any potential issues with name splitting
    missing_first = df['first name'].isnull().sum()
    missing_last = df['last name'].isnull().sum()
    
    if missing_first > 0 or missing_last > 0:
        print(f"⚠️  Warning: {missing_first} records missing first name, {missing_last} missing last name")
    else:
        print("✅ All names split successfully")
    
    return df

def validate_cleaned_data(df):
    """
    Perform final validation on the cleaned dataset to ensure quality.
    This step helps catch any unexpected issues from the cleaning process.
    """
    print("\n=== Final Data Validation ===")
    
    # Check for missing values in critical columns
    missing_summary = df[['first name', 'last name', 'average mark', 'gender']].isnull().sum()
    print("Missing values in key columns:")
    for column, missing_count in missing_summary.items():
        if missing_count > 0:
            print(f"  {column}: {missing_count} missing")
        else:
            print(f"  {column}: No missing values ✅")
    
    # Display final dataset summary
    print(f"\nFinal dataset shape: {df.shape[0]} rows, {df.shape[1]} columns")
    print(f"Final columns: {list(df.columns)}")
    
    return True

def main():
    """
    Main function that orchestrates the complete data cleaning workflow.
    This demonstrates how to structure a multi-step data processing pipeline.
    """
    print("=== Student Data Cleaning Pipeline ===")
    
    # Step 1: Load and validate original data
    df = load_and_validate_data()
    if df is None:
        print("❌ Cannot proceed without valid data")
        return
    
    # Step 2: Remove records with missing average marks
    df_cleaned = remove_missing_average_marks(df)
    
    # Step 3: Split student names into separate columns
    df_final = split_student_names(df_cleaned)
    
    # Step 4: Validate the final cleaned dataset
    validation_passed = validate_cleaned_data(df_final)
    
    if validation_passed:
        # Step 5: Save cleaned data to CSV file
        print(f"\n=== Saving Cleaned Data ===")
        try:
            df_final.to_csv(OUTPUT_FILE, index=False)
            print(f"✅ Cleaned data saved to {OUTPUT_FILE}")
            print(f"Ready for database insertion: {len(df_final)} records")
        except Exception as e:
            print(f"❌ Error saving cleaned data: {e}")
    
    print("\n=== Data Cleaning Complete ===")

if __name__ == "__main__":
    main()