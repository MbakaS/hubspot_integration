import csv
import psycopg2
import random
import time
import datetime
from src import postgres as db

def create_tables(users, serials,memberships,organizations):
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

        # Create Oragnizations table in Cloud DB
        db.cloud_db('''
                    CREATE TABLE IF NOT EXISTS "Organizations" (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100) NOT NULL,
                        identifier VARCHAR(100) NOT NULL,
                        "createdAt" timestamp without time zone NOT NULL,
                        "customerId" character varying NOT NULL
                        )
                    ''', None)

        # Insert data into Users table
        for organization in organizations:
            db.cloud_db('INSERT INTO "Organizations" (name,identifier,"createdAt","customerId") VALUES (%s, %s,%s,%s)', (organization[0], organization[1], organization[2],organization[3]))



        # Create Memberships table in Cloud DB
        db.cloud_db('''
                    CREATE TABLE IF NOT EXISTS "OrganizationMemberships" (
                        id SERIAL PRIMARY KEY,
                        "UserId" serial NOT NULL,
                        "OrganizationId" VARCHAR(100) NOT NULL,
                        role VARCHAR(100) NOT NULL,
                        "isPrimary" boolean NOT NULL,
                        "isContributor" boolean NOT NULL,
                        "createdAt" timestamp without time zone NOT NULL,
                        email VARCHAR(100) NOT NULL,
                        updated timestamp without time zone NOT NULL
                    )
                    ''', None)

        # Insert data into Users table
        for member in memberships:
            db.cloud_db('INSERT INTO "OrganizationMemberships" ("UserId","OrganizationId",role,"isPrimary","isContributor","createdAt",email,updated) VALUES (%s, %s,%s,%s, %s,%s,%s,%s)', (member[0], member[1], member[2],member[3], member[4], member[5],member[6],member[7]))

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


        # Create Memberships table in Analytics DB
        db.analytics_db("ADD",'''
                    CREATE TABLE memberships (
                        "hubspotID" bigint PRIMARY KEY,
                        member bigint ,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)

        # Insert data into Serials table
        db.analytics_db("ADD","""INSERT INTO memberships ("hubspotID", member,created) VALUES (25865251,12345,'2020-01-01 01:25:46')""", None)

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

        # Insert data into Workspaces table
        db.analytics_db("ADD","""INSERT INTO workspaces ("hubspotID", workspace, customer, created) VALUES (25865251,12345,'cus_0pqelrYNJvkd3O','2020-01-01 01:25:46')""", None)


        # Create Oragnizations table in Cloud DB
        db.payments_db('''
                    CREATE TABLE customers (
                        id character varying PRIMARY KEY,
                        external_id character varying NOT NULL
                        )
                    ''', None)

        # Insert data into Users table
        with open('sample_data/customers.csv', 'r') as csvfile:  # Assuming your CSV file is named 'data.csv'
            csvreader = csv.reader(csvfile)
            next(csvreader)  # Skip header row
            i=0
            for row in csvreader:
                customer_id = row[0]
                external_id = row[1]

                # Insert data into the customers table
                db.payments_db('INSERT INTO customers (id, external_id) VALUES (%s, %s)', ( i,customer_id))
                i=i+1

        print("Database bootstrapped successfully.")

    except psycopg2.Error as e:
        print("Error: Unable to bootstrap the database.")
        print(e)

def drop_tables():
    """Drop tables if they exist in Cloud, Legacy, and Analytics databases."""
    try:
        # Drop the Users table in Cloud DB
        db.cloud_db('DROP TABLE IF EXISTS "Users"', None)
        db.cloud_db('DROP TABLE IF EXISTS "OrganizationMemberships"', None)
        db.cloud_db('DROP TABLE IF EXISTS "Organizations"', None)

        # Drop the Legacy table
        db.legacy_db('DROP TABLE IF EXISTS serials', None)
        
        # Drop the Contacts and Serials tables in Analytics DB
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS contacts', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS serials', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS memberships', None)
        db.analytics_db("DELETE",'DROP TABLE IF EXISTS workspaces', None)


        db.payments_db('DROP TABLE IF EXISTS customers', None)
        print("Tables dropped successfully.")
    except psycopg2.Error as e:
        print("Error: Unable to drop the tables.")
        print(e)

num_records = 500

# Generate records for users
users = []
created_at = 1665433278
for i in range(num_records):
    username = f'user{i}'
    email = f'user{i}@sketch.com'
    created_at = created_at + 86400 
    created = datetime.datetime.utcfromtimestamp(created_at)
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

# Generate records for Memberships
memberships = []
created = 1665433278
for i in range(num_records):
    user_id = random.randint(1, 200)  # Replace with appropriate user ID generation logic
    organization_id = random.randint(1,20)  
    role = random.choice(["Admin", "Member", "Guest","Finance"])
    is_primary = random.choice([True, False])
    is_contributor = random.choice([True, False])
    created= created + 86400
    accepted_at = datetime.datetime.utcfromtimestamp(created + 106400)
    created_at = datetime.datetime.utcfromtimestamp(created + 86400)
    updated = datetime.datetime.utcfromtimestamp(created + 2629743)
    email = f'user{i}@sketch.com'  # Generating a dummy email address based on user ID
    memberships.append((user_id, organization_id, role, is_primary, is_contributor,created_at,email,updated))

# Generate and insert 200 records
organizations = []
created = 1665433278
i=0
with open('sample_data/customers.csv', 'r') as csvfile:  # Assuming your CSV file is named 'data.csv'
    csvreader = csv.reader(csvfile)
    next(csvreader)  # Skip header row
    for row in csvreader:
        name = f'Workspace {i}'
        identifier = ''.join([''.join(random.choices('0123456789abcdefghijk', k=4)) for j in range(6)])
        created= created + 86400
        created_at = datetime.datetime.utcfromtimestamp(created + 86400)
        organizations.append((name, identifier, created_at, i))
        i=i+1



# Use this command to drop all the tables created
drop_tables()
# Bootstrap the database
create_tables(users, serials,memberships,organizations)
