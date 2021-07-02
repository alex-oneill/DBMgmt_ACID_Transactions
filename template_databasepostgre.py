import psycopg2
from tabulate import tabulate

con = psycopg2.connect(
    host="localhost",
    database="production",
    user="postgres",
    password="*****")



#For isolation: SERIALIZABLE
con.set_isolation_level(3)
#For atomicity
con.autocommit = False

try:
    cur = con.cursor()
    # QUERY

except (Exception, psycopg2.DatabaseError) as err:
    print(err)
    print("Transactions could not be completed so database will be rolled back before start of transactions")
    con.rollback()
finally:
    if con:
        con.commit()
        cur.close
        con.close
        print("PostgreSQL connection is now closed")