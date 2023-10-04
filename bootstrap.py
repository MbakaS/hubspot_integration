import psycopg2
import random
import time
import datetime
from src import postgres as db

def create_tables(users, serials):
    """
    Create tables for Users, Legacy, and Analytics databases and populate them with data.

    Args:
        users (list): List of tuples containing user data.
        serials (list): List of tuples containing serial data.
    """
    try:
        # Create Users table in Cloud DB
        db.cloud_db('''
                    CREATE TABLE IF NOT EXISTS "Users" (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(50) NOT NULL,
                        email VARCHAR(100) NOT NULL,
                        "createdAt" timestamp NOT NULL
                    )
                    ''', None)

        # Insert data into Users table
        for user in users:
            db.cloud_db('INSERT INTO "Users" (name, email,"createdAt") VALUES (%s, %s,%s)', (user[0], user[1], user[2]))

        # Create Legacy table
        db.legacy_db('''
                    CREATE TABLE IF NOT EXISTS serials (
                        id SERIAL PRIMARY KEY,
                        serial VARCHAR(50) NOT NULL,
                        email VARCHAR(100) NOT NULL,
                        maxUse int NOT NULL,
                        date  VARCHAR(100) NOT NULL,
                        update_expirationdate VARCHAR(100) NOT NULL,
                        last_updated_on timestamp NOT NULL
                    )
                    ''', None)

        # Insert data into Legacy table
        for serial in serials:
            db.legacy_db('INSERT INTO serials (serial, email,maxUse,date,update_expirationdate,last_updated_on) VALUES (%s, %s,%s,%s,%s,%s)',
                         (serial[0], serial[1], serial[2], serial[3], serial[4], serial[5]))

        # Create Contacts table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE contacts (
                        "hubspotID" bigint PRIMARY KEY,
                        email character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        # Insert data into Contacts table
        db.analytics_db("ADD","""INSERT INTO contacts ("hubspotID", email,created) VALUES (25865251,'user@rotterdam.com','2020-01-01 01:25:46')""", None)

        # Create Serials table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE serials (
                        "hubspotID" bigint PRIMARY KEY,
                        serial character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        # Insert data into Serials table
        db.analytics_db("ADD","""INSERT INTO serials ("hubspotID", serial,created) VALUES (25865251,'12E7-B8FE-B573-A01C-88F4-4CD5','2020-01-01 01:25:46')""", None)

        print("Database bootstrapped successfully.")

    except psycopg2.Error as e:
        print("Error: Unable to bootstrap the database.")
        print(e)

def drop_tables():
    """Drop tables if they exist in Cloud, Legacy, and Analytics databases."""
    try:
        # Drop the Users table in Cloud DB
        db.cloud_db('DROP TABLE IF EXISTS "Users"', None)
        
        # Drop the Legacy table
        db.legacy_db('DROP TABLE IF EXISTS serials', None)
        
        # Drop the Contacts and Serials tables in Analytics DB
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS contacts', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS serials', None)
        
        print("Tables dropped successfully.")
    except psycopg2.Error as e:
        print("Error: Unable to drop the tables.")
        print(e)

num_records = 200

# Generate records for users
users = []
for i in range(num_records):
    username = f'user{i}'
    email = f'user{i}@sketch.com'
    random_date = int(time.time()) - random.randint(0, 31536000)  # Random timestamp within the last year
    created = datetime.datetime.utcfromtimestamp(random_date)
    users.append((username, email, created))

# Generate records for serials
serials = []
created_at = 1665433278
for i in range(num_records):
    uuid = '-'.join([''.join(random.choices('0123456789ABCDEF', k=4)) for _ in range(6)])
    email = f'user{i}@rotterdam.com'
    age = random.randint(18, 99)
    created_at = created_at + 86400  # Random timestamp within the last year
    update_expirationdate = created_at + 31556926
    last_updated_on = datetime.datetime.utcfromtimestamp(created_at + 2629743)
    serials.append((uuid, email, age, created_at, update_expirationdate, last_updated_on))

# Bootstrap the database
create_tables(users, serials)

# Use this command to drop all the tables created
# drop_tables()
