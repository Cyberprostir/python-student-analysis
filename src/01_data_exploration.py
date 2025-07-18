import pandas as pd
import os

#Configuration - defining file paths and settings
DATA_FILE = 'data/students.xlsx'
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

def main():
    """
    Main function to perform data exploration on student dataset.
    """
    print("===Student Data Exploration Report ===")
    print(f"Project root: {PROJECT_ROOT}")
    #Step 1: Load the data
    print("\n--- Loading Data ---")
    
    
    try:
        # Load the Excel file into a pandas DataFrame
        df = pd.read_excel(DATA_FILE)
        print(f"✅ Successfully loaded {len(df)} records from {DATA_FILE}")
        
        # Step 2: Basic dataset information
        print("\n--- Dataset Overview ---")
        print(f"Dataset shape: {df.shape[0]} rows, {df.shape[1]} columns")
        print(f"Column names: {list(df.columns)}")
        
        # Step 3: Data types and memory usage
        print("\n--- Data Types Analysis ---")
        print(df.dtypes)
        
        # Step 4: First few records preview
        print("\n--- Sample Data (First 5 Records) ---")
        print(df.head())
        
        # Step 5: Missing values check
        print("\n--- Data Quality Assessment ---")
        missing_values = df.isnull().sum()
        print("Missing values per column:")
        for column, missing_count in missing_values.items():
            if missing_count > 0:
                print(f"  {column}: {missing_count} missing ({missing_count/len(df)*100:.1f}%)")
            else:
                print(f"  {column}: No missing values ✅")
                  
    except FileNotFoundError:
        print(f"❌ Error: Could not find the file {DATA_FILE}")
        print("Please check that students.xlsx is in the data folder")
    except Exception as e:
        print(f"❌ Error loading data: {e}")
  
  
if __name__ == "__main__":
    main()