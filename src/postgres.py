""" This module handles all database connections"""
import os
import psycopg2
import psycopg2.extras
import mysql.connector
from dotenv import load_dotenv, find_dotenv

# Load environment variables from a .env file
load_dotenv(find_dotenv())

def analytics_db(action, query, values):
    """
    Perform database operations on the analytics database.

    Args:
        action (str): The action to perform (UPDATE, GET, ADD, DELETE).
        query (str): The SQL query to execute.
        values (tuple): A tuple of values for the query placeholders.

    Returns:
        Depends on the action:
        - For UPDATE, ADD, and DELETE, returns "Records updated".
        - For GET, returns the fetched records as a list of tuples.
    """
    conn = None
    response = None
    if action in ('UPDATE', 'GET', 'ADD', 'DELETE'):
        try:
            params = {
                'dbname': os.getenv('ANALYTICS_NAME'),
                'user': os.getenv('ANALYTICS_USER'),
                'password': os.getenv('ANALYTICS_PASSWORD'),
                'host': os.getenv('ANALYTICS_HOST'),
                'port': os.getenv('ANALYTICS_PORT'),

            }
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
            print(f"Error in analytics DB connection: {error}")
        finally:
            if conn is not None:
                conn.close()
    else:
        print("No such database action")
    return response

def payments_db(query, values):
    """
    Perform database operations on the payments database.

    Args:
        query (str): The SQL query to execute.
        values (tuple): A tuple of values for the query placeholders.

    Returns:
        The fetched records as a list of tuples.
    """
    conn = None
    response = None
    try:
        params = {
            'dbname': os.getenv('PAYMENTS_NAME'),
            'user': os.getenv('PAYMENTS_USER'),
            'password': os.getenv('PAYMENTS_PASSWORD'),
            'host': os.getenv('PAYMENTS_HOST'),
        }
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        cur.execute(query, values)
        response = cur.fetchall()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error in Payments DB connection: {error}")
    finally:
        if conn is not None:
            conn.close()
    return response

def cloud_db(query, values):
    """
    Perform database operations on the cloud database.

    Args:
        query (str): The SQL query to execute.
        values (tuple): A tuple of values for the query placeholders.

    Returns:
        The fetched records as a list of tuples.
    """
    conn = None
    response = None

    try:
        params = {
            'dbname': os.getenv('CLOUD_NAME'),
            'user': os.getenv('CLOUD_USER'),
            'password': os.getenv('CLOUD_PASSWORD'),
            'host': os.getenv('CLOUD_HOST'),
            'port': os.getenv('CLOUD_PORT'),
        }
        conn = psycopg2.connect(**params, cursor_factory=psycopg2.extras.DictCursor)
        cur = conn.cursor()
        cur.execute(query, values)
        conn.commit()
        response = cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error in CLOUD database connection: {error}")
    finally:
        if conn is not None:
            conn.close()
    return response

def legacy_db(query, values):
    """
    Perform database operations on the licensing database.

    Args:
        query (str): The SQL query to execute.
        values (tuple): A tuple of values for the query placeholders.

    Returns:
        The fetched records as a list of tuples.
    """
    conn = None
    response = None
    try:
        params = {
            'database': os.getenv('LEGACY_NAME'),
            'user': os.getenv('LEGACY_USER'),
            'password': os.getenv('LEGACY_PASSWORD'),
            'host': os.getenv('LEGACY_HOST'),
            'port': os.getenv('LEGACY_PORT'),
        }
        conn = mysql.connector.connect(**params)
        cur = conn.cursor()
        cur.execute(query, values)
        response = cur.fetchall()

    except (mysql.connector.Error) as error:
        print(f"Error in LEGACY connection: {error}")
    finally:
        if conn is not None:
            conn.commit()
            conn.close()
    return response
