import os
import psycopg2
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

def get_db_params():
    db_params = {
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
    }
    return db_params

def conn(action, query, values):
    conn = None
    response = None
    if action in ('UPDATE', 'GET', 'ADD', 'DELETE'):
        try:
            params = get_db_params()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()

            if values is None:
                cur.execute(query)
                response = "Records updated"
            else:
                cur.execute(query, values)
                response = "Records updated"

            if action == 'GET':
                response = cur.fetchall()

            conn.commit()
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(f"Error in db connection: {error}")
        finally:
            if conn is not None:
                conn.close()
    else:
        print("No such Database action")
    return response

# Example usage:
# result = conn('GET', 'SELECT * FROM your_table', None)
# print(result)
