import psycopg2
import pandas as pd
import sys

def table_to_csv(sql, file_path, conn):

    """This function creates a csv file from PostgreSQL with query
    """
    try:
        # Get data into pandas dataframe
        df = pd.read_sql(sql, conn)
        # Write to csv file
        df.to_csv(file_path, encoding='utf-8', header = True,doublequote = True, sep=',', index=False)
        print("CSV File has been created")
        conn.close()

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

# connection parameter
try:
    conn = psycopg2.connect(
        host="localhost",
        database="mydatabase",
        user="rm_user",
        password="mypassword"
    )
except:
    print("Connection to database failed", file=sys.stderr)
    sys.exit(-1)

#sql parameter
sqlquery = ("SELECT * FROM airlines_final1")
file_path = ('my_new_file.csv')
table_to_csv(sqlquery, file_path, conn)
