import psycopg2
from configparser import ConfigParser
#from tabulate import tabulate

#con = psycopg2.connect(
#    host="localhost",
#    database="production",
#    user="postgres",
#    password="*****")

def config(filename='./database.ini', section='postgres'):
    """Establishes connection to local postgres db with masked credentials."""
    parser = ConfigParser()
    parser.read(filename)
    db_conn = {}
    for param in parser.items(section):
        db_conn[param[0]] = param[1]
    return db_conn


try:
    params = config()
    conn = psycopg2.connect(**params)
    #For isolation: SERIALIZABLE
    conn.set_isolation_level(3)
    #For atomicity
    conn.autocommit = False
    cur = conn.cursor()
    # QUERY
    cur.execute("""SELECT p.p_name
            FROM product p, stock s
            WHERE p.prod_id = s.prod_id 
            AND s.dep_id = 'd2';""")
    data = cur.fetchall()
    print(data)
    
    

except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back before start of transactions")
    conn.rollback()
finally:
    if conn:
        conn.commit()
        cur.close
        conn.close
        print("PostgreSQL connection is now closed")