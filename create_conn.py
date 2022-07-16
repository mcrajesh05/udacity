import psycopg2


def create_connection(db_name, user_name, password):
    """
    Function to make database connection and return cursor and connection context.
    - db_name  : Database name to connect
    - user_name : User connecting to database
    - password :  Password to connect to database
    """
    try:
        conn = psycopg2.connect(f"host=127.0.0.1 dbname={db_name} user={user_name} password={password}")
    except psycopg2.Error as err:
        print(f"Error: Could not make connection to the Postgres database. Reason : {err}")

    try:
        cur = conn.cursor()
    except psycopg2.Error as err:
        print(f"Error: Could not get cursor to the Database. Reason : {err}")
    conn.set_session(autocommit=True)

    return cur, conn
