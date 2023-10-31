""" Create analytics tables"""
import psycopg2
from src import postgres as db

def create_tables():
    """
    Create tables for Users, Legacy, and Analytics databases and populate them with data.

    Args:
        users (list): List of tuples containing user data.
        serials (list): List of tuples containing serial data.
    """
    try:
        # Create Contacts table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE contacts (
                        "hubspotID" bigint PRIMARY KEY,
                        email character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        # Create Serials table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE serials (
                        "hubspotID" bigint PRIMARY KEY,
                        serial character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        # Create Memberships table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE memberships (
                        "hubspotID" bigint PRIMARY KEY,
                        member bigint ,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        # Create Workspaces table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE workspaces (
                        "hubspotID" bigint PRIMARY KEY,
                        workspace bigint ,
                        customer character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        print("Database bootstrapped successfully.")

    except psycopg2.Error as e:
        print("Error: Unable to bootstrap the database.")
        print(e)

def drop_tables():
    """Drop tables if they exist in Cloud, Legacy, and Analytics databases."""
    try:
        # Drop the Contacts and Serials tables in Analytics DB
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS contacts', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS serials', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS memberships', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS workspaces', None)

    except psycopg2.Error as e:
        print("Error: Unable to drop the tables.")
        print(e)

# Use this command to drop all the tables created
drop_tables()
# Bootstrap the database
create_tables()
