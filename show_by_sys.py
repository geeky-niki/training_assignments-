import sys
import psycopg2
import snowflake.connector

print(sys.argv)
print(len(sys.argv))

# Retrieve the database types from the command line
db1 = sys.argv[1]

def postgres_data():
    connection = psycopg2.connect(
        host='localhost',
        port='5432',
        database='postgres',
        user='postgres',
        password='pgadmin'
    )
    cur = connection.cursor()
    cur.execute("SELECT * FROM patient ORDER BY 1")
    data = cur.fetchall()
    for row in data:
        print(row)

    cur.close()
    connection.close()

def snowflake_data():
    snowflake_conn = snowflake.connector.connect(
        user='cn03',
        password='Doluv2bluvd',
        account='kbefsvr-tm08737',
        warehouse="COMPUTE_WH",
        database='ATI_TRAINING_DB',
        schema='ATI_ST_SCHEMA'
    )

    snowflake_cur = snowflake_conn.cursor()
    snowflake_cur.execute("SELECT * FROM sf_py_data")
    data = snowflake_cur.fetchall()
    for row in data:
        print(row)

    snowflake_cur.close()
    snowflake_conn.close()

if len(sys.argv) == 2:
    if db1.lower() == "postgres":
        postgres_data()
    elif db1.lower() == "snowflake":
        snowflake_data()
    else:
        print("Invalid database type. Use 'postgres' or 'snowflake'.")

elif len(sys.argv) == 3:
    db2 = sys.argv[2]
    if db1.lower() == "postgres" and db2.lower() == "snowflake":
        postgres_data()
        snowflake_data()
    elif db1.lower() == "snowflake" and db2.lower() == "postgres":
        snowflake_data()
        postgres_data()
    else:
        print("Invalid combination of database types. Use 'postgres snowflake' or 'snowflake postgres'.")

else:
    print("Invalid number of arguments.")
