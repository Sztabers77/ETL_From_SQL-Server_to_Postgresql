from sqlalchemy import create_engine
import pyodbc
import pandas as pd
import os

#get creds from env vars

pwd = os.environ.get("PGPASS")  # demopass
uid = os.environ.get("PGUID")  # etl

#sql db details

driver = "{ODBC Driver 18 for SQL Server}"
server = "DESKTOP-VABF3RT\MSSQLSERVER02"
database = "AdventureWorks2022"

# Extract data from SQL Server

def extract():
    src_conn = None  # Inicjalizacja zmiennej src_conn
    try:
        src_conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
        src_cursor = src_conn.cursor()
        # execute query
        src_cursor.execute(""" select  t.name as table_name from sys.tables t where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            # query and load data to a dataframe
            df = pd.read_sql_query(f'SELECT * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:
        print("Data extract error: " + str(e))
    finally:
        if src_conn:
            src_conn.close()  # Zamknięcie połączenia tylko jeśli zostało utworzone

# Load data to PostgreSQL

def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/demo')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        # save df to postgres
        df.to_sql(f'stg_{tbl}', engine, if_exists="replace", index=False)
        rows_imported += len(df)
        # add elapsed time to final print out
    except Exception as e:    
        print("Data load error: " + str(e))

try:
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))
