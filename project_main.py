"""
Database Management Systems - Prof. Scharff (Summer 2021)
Final Project
Alex ONeill
Maryia Kalodkina
"""
import psycopg2
from configparser import ConfigParser


# SECTION: FUNCTIONS
def config(filename='./database.ini', section='postgres'):
    """
    Ingests database connection parameters with masked credentials from a database.ini file in the below format:
        [postgres]
        host=localhost
        database=database_name
        user=username
        password=password
    """
    parser = ConfigParser()
    parser.read(filename)
    db_config = {}
    for param in parser.items(section):
        db_config[param[0]] = param[1]
    return db_config


def connect(con_str):
    """Establishes connection to the database"""
    try:
        connection = psycopg2.connect(**con_str)
        return connection
    except Exception as conn_err:
        print(conn_err)
        print('Unable to connect to database. Aborting')


# SECTION: MAIN
params = config()
conn = connect(params)
cur = conn.cursor()

if conn:
    try:
        # NOTE: Set to serialize for transaction ISOLATION
        conn.set_isolation_level(3)

        # NOTE: No autocommit to instill ATOMICITY
        conn.autocommit = False

        # TODO: QUERY
        cur.execute("""SELECT p.p_name
                FROM product p, stock s
                WHERE p.prod_id = s.prod_id
                AND s.dep_id = 'd2';""")
        # TODO: REMOVE THESE
        data = cur.fetchall()
        print(data)
    except (Exception, psycopg2.DatabaseError) as err:
        print(err)
        print("Transactions could not be completed so database will be rolled back before start of transactions")
        conn.rollback()
    finally:
        conn.commit()
        cur.close
        conn.close
        print("PostgreSQL connection is now closed")
