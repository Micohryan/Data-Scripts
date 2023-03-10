
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import numpy as np
from dotenv import load_dotenv
import os
import sys
load_dotenv()

def execute_values(conn, df, table):
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
  

def getColumnDtypes(dataTypes):
    '''
    Function to create list of data types
    required for database table
    '''
    dataList = []
    for x in dataTypes:
        if(x == 'int64'):
            dataList.append('int')
        elif (x == 'float64'):
            dataList.append('float')
        elif (x == 'bool'):
            dataList.append('boolean')
        else:
            dataList.append('varchar')
    return dataList

# Reaf csv files into dataframe
resultDF = pd.read_csv("airlines_final.csv")

# Collect column names into a list
columnName = list(resultDF.columns.values)
# Collect column data types into a list
columnDataType = getColumnDtypes(resultDF.dtypes)


# Code for create table statement 
createTableStatement = 'CREATE TABLE IF NOT EXISTS airlines_final1 ('
for i in range(len(columnDataType)):
    createTableStatement = createTableStatement + '\n' + columnName[i] + ' ' + columnDataType[i] + ','
createTableStatement = createTableStatement[:-1] + ' );'

# Connect to database server to run create table statement
try:

    conn = psycopg2.connect(
        host= os.getenv("host"),
        database= os.getenv("database"),
        user=os.getenv("user"),
        password=os.getenv("password")
    )
except:
    print("Connection to database failed", file=sys.stderr)
    sys.exit(-1)

cur = conn.cursor()
cur.execute(createTableStatement)
execute_values(conn, resultDF, 'airlines_final1')

