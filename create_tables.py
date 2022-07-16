import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from create_conn import create_connection


def create_database():

    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """

    # Connect to default database
    print("Connecting to DB studentdb")
    cur, conn = create_connection("studentdb", "student", "student")

    # Create sparkify database with UTF8 encoding
    try:
        print("BEGIN : Creating database sparkifydb")
        cur.execute("DROP DATABASE IF EXISTS sparkifydb")
        cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")
        print("END : Database sparkifydb created")
    except psycopg2.Error as err:
        print(f"Error: Failed to create database. Reason : {err}")

    # Close connection to default database
    conn.close()
    print("Closed connection to DB studentdb")

    print("Connecting to DB sparkifydb")
    # Now ,connect to sparkifydb database
    cur, conn = create_connection("sparkifydb", "student", "student")

    # Create SCHEMA: sparkify and provide ownership or authorisation to user "student"
    try:
        print("BEGIN : Creating schema sparkify")
        cur.execute("CREATE SCHEMA IF NOT EXISTS sparkify AUTHORIZATION student")
        print("END : Schema sparkify created")
    except psycopg2.Error as err:
        print(f"Error: Failed to create schema. Reason : {err}")

    return cur, conn


def drop_tables(cur, conn):

    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    print("BEGIN -> Dropping Tables")
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print("END-> All Tables Dropped")


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    print("BEGIN -> Creating Tables")
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()
    print("END -> Created all the tables")


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection.
    """
    print("##### BEGIN main #####")
    # Establishes  connection  with the sparkify database and gets  cursor to it.
    cur, conn = create_database()

    # Drop the tables
    drop_tables(cur, conn)

    # Create the tables
    create_tables(cur, conn)

    # Closes the connection
    print("Closed connection to DB sparkifydb")
    conn.close()
    print("##### End main #####")


if __name__ == "__main__":
    main()
