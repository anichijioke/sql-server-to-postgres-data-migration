# Preface

In this project we will be migrating data from sql server into postgres.

Tools used:

- Python
- SQL
- Jupyter notebook

# Pseudocode
## High-level

1. Audit the data into SQL Server (before migration)
2. Extract data from SQL Server (SSMS)
3. Transform the data
4. Load the data in PostgreSQL
5. Generate a validation report

## Low-Level

- Create a .env file
- Load env avariables
- Connect to SQL Server (pyodbc)
- Connect to Postgres (psycopg2)
- Audit the data
- For each table
    - Get row count
    - Extract all the rows
    - Transform the column name to lowercase 
    - convert the data types
    - Create tables in Postgres
    - Load tables into Postgres
- Run post data migration check
- Generate validation report 