Student Data Analysis Pipeline


Assignment Requirements
The assignment required completing these specific tasks:

1. Create a 'students' table in PostgreSQL with an auto-incrementing primary key
2. Read data from students.xlsx and export it to text format (CSV)
3. Remove rows where 'average mark' is missing
4. Split 'student name' into separate 'first name' and 'last name' columns
5. Store database connection settings in a separate JSON configuration file
6. Insert the cleaned data into the PostgreSQL database
7. Count male and female students with average marks greater than 5.0 and display results in a pandas DataFrame


Student Data Analysis Pipeline
Project Overview
This project implements a comprehensive data analysis workflow that processes student information from Excel format, stores it in a PostgreSQL database, and performs analytical queries to answer questions about student performance patterns. The solution demonstrates integration between Python data processing, PostgreSQL database management, and professional software development practices.

Assignment Requirements Fulfilled
This implementation addresses all specified assignment requirements:

Database Schema Creation: Establishes PostgreSQL table with auto-incrementing primary key
Data Export: Converts Excel data to text format (CSV) for processing
Data Quality Management: Removes records with missing critical information
Data Transformation: Separates combined name fields into discrete components
Configuration Management: Stores database settings in external JSON configuration
Database Integration: Loads processed data into PostgreSQL database
Analytical Queries: Counts students by demographic and performance criteria, presenting results in pandas DataFrame format

Technical Architecture
Data Processing Pipeline
The project implements a five-stage data processing pipeline that demonstrates systematic approaches to data analysis workflows:
Stage 1: Data Exploration (01_data_exploration.py)

Examines original dataset structure and characteristics
Identifies data quality issues and processing requirements
Provides statistical summaries and data type analysis

Stage 2: Data Cleaning (02_data_cleaning.py)

Removes records with missing critical information
Transforms combined data fields into normalized components
Exports cleaned dataset in standardized format

Stage 3: Database Infrastructure (03_database_setup.py)

Creates database schema with appropriate data types and constraints
Implements configuration management for database connections
Establishes secure database access patterns

Stage 4: Data Integration (04_data_insertion.py)

Loads processed data into database with transaction safety
Validates data integrity throughout insertion process
Provides comprehensive feedback on operation success

Stage 5: Analytical Processing (05_data_analysis.py)

Executes analytical queries to answer performance questions
Converts database results to pandas DataFrame format
Presents findings with additional context and insights

Database Schema Design
The database implementation uses a normalized table structure with these characteristics:

Primary Key: Auto-incrementing integer identifier
Data Integrity: Appropriate data types for each field (VARCHAR, INTEGER, NUMERIC, BIGINT)
Audit Trail: Automatic timestamp generation for record creation
Indexing: Primary key indexing for efficient query performance

Security Implementation
This project demonstrates security best practices for database-driven applications by implementing proper credential management and access control principles.
Database Security Approach
The project uses dedicated database credentials with minimal required permissions rather than administrator accounts. This follows the principle of least privilege by providing only the database access needed for the specific analytical operations.
Configuration Management
Database connection settings are stored in a separate JSON configuration file that is excluded from version control to prevent credential exposure. The repository includes a template file that shows the required configuration structure without exposing actual credentials.

Setup for New Users
To run this project in your environment:

Create a dedicated PostgreSQL user account for this project
Grant the user appropriate permissions for database and table creation
Copy database_config_template.json to database_config.json
Update the configuration file with your specific database credentials
Ensure the configuration file is never committed to version control

This approach ensures that each user implements proper security practices while maintaining the ability to reproduce the analytical workflow in their own environment.
Data Processing Results
Dataset Characteristics

Original Records: 1,000 student entries
Data Quality Issues: 7 records with missing performance data
Final Dataset: 993 complete records processed successfully
Data Transformation: All student names successfully separated into discrete components

Key Analytical Findings
The analytical queries revealed these patterns about student performance:
High-Performing Students (Average Mark > 5.0)

Female students meeting criteria: 440 (53.5% of high performers)
Male students meeting criteria: 382 (46.5% of high performers)
Total students above performance threshold: 822 out of 993 (82.8%)

Performance Distribution Insights
The analysis demonstrates that the majority of students achieve strong academic performance, with more than 4 out of 5 students exceeding the 5.0 performance threshold. The distribution between demographic groups shows relatively balanced high performance rates, with slight variation in the absolute numbers achieving the performance criteria.

Technical Requirements
Software Dependencies

Python 3.7 or higher
PostgreSQL database server

Required Python packages (see requirements.txt):

pandas (data manipulation and analysis)
psycopg2-binary (PostgreSQL database connectivity)
openpyxl (Excel file processing)



Environment Setup

Virtual Environment: Create isolated Python environment to prevent package conflicts
Database Server: PostgreSQL installation with appropriate user permissions
Package Installation: Install required dependencies using pip and requirements.txt
Configuration: Create database configuration file based on provided template

Project Structure
```
python_student_analysis/
├── src/                          # Python scripts implementing the analysis pipeline
│   ├── 01_data_exploration.py    # Dataset examination and quality assessment
│   ├── 02_data_cleaning.py       # Data transformation and cleaning operations
│   ├── 03_database_setup.py      # Database schema creation and configuration
│   ├── 04_data_insertion.py      # Data loading and validation procedures
│   └── 05_data_analysis.py       # Analytical queries and results presentation
├── data/                         # Dataset files
│   └── students.xlsx             # Original dataset for analysis
├── config/                       # Configuration management
│   └── database_config_template.json  # Database connection template
├── README.md                     # Project documentation
└── requirements.txt              # Python package dependencies
```
Running the Analysis
Execute the scripts in numerical order to complete the full analytical workflow:

Data Exploration: python src/01_data_exploration.py
Data Cleaning: python src/02_data_cleaning.py
Database Setup: python src/03_database_setup.py
Data Integration: python src/04_data_insertion.py
Analysis Execution: python src/05_data_analysis.py

Each script includes comprehensive error handling and provides detailed feedback about operation progress and results.

Professional Development Practices
This project demonstrates several software development best practices:
Modular Design: Separate scripts for distinct workflow phases enable maintainable, testable code
Error Handling: Comprehensive validation and error management throughout all operations
Documentation: Clear code comments and professional project documentation
Security Awareness: Proper credential management and sensitive data protection
Configuration Management: Separation of environment-specific settings from application logic


