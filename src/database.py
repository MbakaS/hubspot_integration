""" Contains all hubspot operations"""
import re
import datetime
import pandas as pd
import postgres as db


def get_contacts():
    """
    Fetch new contacts from the database.

    This function retrieves new contacts from the Users table whose "createdAt" timestamp
    is greater than the last synchronization timestamp obtained 
    from the contacts table or triggers a full sync if the table is empty.

    Returns:
        list: A list of new contacts, each represented as a tuple (email, name).
    """
    cloud_users = []
    serial_users = []
    sso_users = []
    try:
        # Fetch the last sync timestamp from the database
        last_sync_query = 'select max(created) from contacts'
        last_sync_timestamp = db.analytics_db("GET",last_sync_query, None)
        if last_sync_timestamp[0][0] is None:
            print("Contacts table empty, start full Sync")
            # create tables if they do not exist
            db.analytics_db("ADD",'''
                    CREATE TABLE if not exists contacts (
                        "hubspotID" bigint PRIMARY KEY,
                        email character varying,
                        type character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)
            db.analytics_db("ADD",'''
                    CREATE TABLE if not exists invalid_contacts (
                        id serial PRIMARY KEY,
                        email character varying
                    )
                    ''', None)
            user_query = """select email, date from emails where email is not null
            group by 1,2 order by date limit 500000"""
            cloud_users = db.analytics_db("GET",user_query,None)
            cloud_list = [(re.sub(r"'", r"''", email), date, 'cloud') for email, date in cloud_users]

            sso_query = """select email, "createdAt" from "ExternalIdentities"
            where email is not null group by 1,2 order by "createdAt" asc """
            sso_users = db.cloud_db(sso_query,None)
            sso_list = [(email, date, 'sso') for email, date in sso_users]

            user_licenses = '''select email, max(FROM_UNIXTIME(date)) as date
            from serials where STR_TO_DATE(FROM_UNIXTIME(expirationdate), '%Y-%m-%d') > CURDATE() 
                    AND STR_TO_DATE(FROM_UNIXTIME(update_expirationdate), '%Y-%m-%d') 
                                > CURDATE() group by 1 '''
            serial_users = db.legacy_db(user_licenses,None)
            serial_list = [(email, date, 'serial') for email, date in serial_users]
        else:
            # Fetch new contacts from Users table
            print("Fetching new Contacts")
            last_sync_query = "select max(created) from contacts where  type = 'cloud'"
            last_sync_timestamp = db.analytics_db("GET",last_sync_query, None)
            user_query = f"""select email, max("createdAt") as "createdAt" from "Users" where email
            is not null and "createdAt" > '{last_sync_timestamp[0][0]}' and email NOT ILIKE '%''%'
              ESCAPE '#' group by 1  order by "createdAt" asc """
            cloud_users = db.cloud_db(user_query,None)
            cloud_list = [(email, date, 'cloud') for email,date in cloud_users]
            print(f"Cloud Users: {len(cloud_list)}")
            last_sync_query = "select max(created) from contacts where  type = 'sso'"
            last_sync_timestamp = db.analytics_db("GET",last_sync_query, None)
            if last_sync_timestamp[0][0] is None:
                sso_query = """select email, "createdAt" from "ExternalIdentities"
                where email is not null and email NOT ILIKE '%''%' ESCAPE '#' group by 1,2 
                order by "createdAt" asc """
                sso_users = db.cloud_db(sso_query,None)
            else:
                sso_query = f"""select email, "createdAt" from "ExternalIdentities"
                where email is not null and "createdAt" > '{last_sync_timestamp[0][0]}' 
                and email NOT ILIKE '%''%' ESCAPE '#' group by 1,2 order by "createdAt" asc """
                sso_users = db.cloud_db(sso_query,None)
            sso_list = [(email, date, 'sso') for email, date in sso_users]
            print(f"SSO Users: {len(sso_users)}")
            #fetch new users from Serials
            last_sync_query = "select max(created) from serials"
            last_sync = db.analytics_db("GET",last_sync_query, None)
            if last_sync[0][0] is not None:
                unix_timestamp = int(last_sync[0][0].timestamp())
                user_licenses = f"""select s.email,FROM_UNIXTIME(s.date) as date from serials s
                where date > '{unix_timestamp}' and STR_TO_DATE(FROM_UNIXTIME(s.expirationdate), '%Y-%m-%d')
                  > CURDATE() AND STR_TO_DATE(FROM_UNIXTIME(s.update_expirationdate), '%Y-%m-%d')
                    > CURDATE() and s.email NOT LIKE '%''%' ESCAPE '\'"""
                serial_users = db.legacy_db(user_licenses,None)
            else:
                user_licenses = """select s.email,FROM_UNIXTIME(s.date) as date from serials s
                where  STR_TO_DATE(FROM_UNIXTIME(s.expirationdate), '%Y-%m-%d') > CURDATE() 
                AND STR_TO_DATE(FROM_UNIXTIME(s.update_expirationdate), '%Y-%m-%d') > CURDATE()
                  and s.email NOT LIKE '%''%' ESCAPE '\'"""
                serial_users = db.legacy_db(user_licenses,None)
            serial_list = [(email, date, 'serial') for email, date in serial_users]
            print(f"Serial Users: {len(serial_users)}")
            print("SUCCESS: New contacts retrieved from Database")
    except Exception as get_exception:
        print(f"Error in Contacts (GET): {get_exception}")
    return cloud_list+sso_list+serial_list

def duplicate_contacts(contacts):
    """
    Remove duplicate contacts based on email addresses.

    Args:
        contacts (list): List of new contacts.

    Returns:
        list: Filtered list without duplicate email addresses.
    """
    contact_list = ', '.join([f"'{contact[0]}'" for contact in contacts])
    query = f'select email from contacts where email in ({contact_list})'
    duplicates = db.analytics_db("GET",query, None)
    email_list = [email[0] for email in duplicates]
    filtered_contacts = [contact for contact in contacts if contact[0] not in email_list]
    return filtered_contacts

def invalid_emails(contacts):
    """
    Insert invalid contacts into the database.

    Args:
        contacts (list): List of invalid contacts.
        batch (int): Batch number.

    Returns:
        bool: True if insertion is successful, False otherwise.
    """
    try:
        query = 'insert into invalid_contacts ( email) values (%s)'
        # Iterate through the list of HubSpot contact IDs and insert them into the database
        for contact in contacts:
            query = f"insert into invalid_contacts ( email) values ('{str(contact)}')"
            db.analytics_db("ADD", query, None)
    except Exception as get_exception:
        print(f"Error in insertHubspotID (contacts): {get_exception}")  # Handle any exceptions
    print("SUCCESS: Invalid Contacts succesfully added to the DB")
    return True

def insert_contact_ids(hubspotids):
    """
    Insert HubSpot contact IDs into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot contact IDs, 
        email addresses, and created timestamps.

    Returns:
        None
    """
    try:
        query = 'insert into contacts ("hubspotID", email,type, created) values (%s, %s, %s,%s)'
        # Iterate through the list of HubSpot contact IDs and insert them into the database
        for contact in hubspotids:        
            values = (int(contact[1]), contact[0], contact[3],contact[2])
            db.analytics_db("UPDATE", query, values)
    except Exception as get_exception:
        print(f"Error in insertHubspotID (contacts): {get_exception}")  # Handle any exceptions
    print("SUCCESS: Contacts succesfully added to the DB")

def delete_contacts(contacts):
    """
    Delete contacts from the database.

    Args:
        contacts (list): A list of email addresses to delete.

    Returns:
        list: A list of HubSpot contact IDs corresponding to the deleted contacts.
    """
    try:
        # Delete the contacts from both the database
        contact_list = ', '.join([f"'{contact[0]}'" for contact in contacts])
        db_query = f'delete from contacts where "hubspotID" in ({contact_list})'
        db.analytics_db("DELETE", db_query, None)
    except Exception as get_exception:
        print(f"Error in deleting HubSpotIDs (GET): {get_exception}")
    return True

def get_serials():
    """
    Retrieve new serials from the database based on the last synchronization timestamp.

    Returns:
        list: A list of dictionaries representing the new serials.
    """
    try:
        print("START: Getting new serials")
        contacts_dict ={}
        new_serials = []
        # Query to retrieve the last sync timestamp from the database
        last_sync_query = "select max(created) from serials"
        last_sync = db.analytics_db("GET",last_sync_query, None)
        if last_sync[0][0] is None:
            print("Serials table empty, start full Sync")
            db.analytics_db("ADD",'''
                    CREATE TABLE if not exists serials (
                        "hubspotID" bigint PRIMARY KEY,
                        email character varying,
                        created timestamp without time zone,
                        updated timestamp without time zone
                    )
                    ''', None)
            serials_query = """
                        select
                            s.serial,
                            s.email,
                            FROM_UNIXTIME(s.date, '%Y-%m-%d') as created,
                            s.id,
                            s.maxUse,
                            FROM_UNIXTIME(s.update_expirationdate, '%Y-%m-%d') as update_expirationdate,
                            IF(
                                STR_TO_DATE(FROM_UNIXTIME(s.expirationdate), '%Y-%m-%d') > CURDATE() 
                                AND STR_TO_DATE(FROM_UNIXTIME(s.update_expirationdate), '%Y-%m-%d') > CURDATE(),
                                'true',
                                'false'
                            ) AS status,
                            FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') as created_long
                            from
                                serials s
                            where STR_TO_DATE(FROM_UNIXTIME(s.expirationdate), '%Y-%m-%d') > CURDATE() 
                                AND STR_TO_DATE(FROM_UNIXTIME(s.update_expirationdate), '%Y-%m-%d') > CURDATE()
                            order by
                                s.date asc 
        """
            new_serials = db.legacy_db(serials_query,None)
            if len(new_serials)>0:
                serial_emails = ', '.join([f"'{serial[1]}'" for serial in new_serials])
                hubspotids = db.analytics_db("GET",f'''
                        select email,"hubspotID" from contacts where email in ({serial_emails})
                        ''', None)
                # Create a dictionary mapping email addresses to hubspot IDs
                contacts_dict = {email: hubspot_id for email, hubspot_id in hubspotids}

        else:
            # Query to retrieve new serials from the database
            serials_query = f"""
                            select
                                s.serial,
                            s.email,
                            FROM_UNIXTIME(s.date, '%Y-%m-%d') as created,
                            s.id,
                            s.maxUse,
                            FROM_UNIXTIME(s.update_expirationdate, '%Y-%m-%d') as update_expirationdate,
                            IF(
                                STR_TO_DATE(FROM_UNIXTIME(s.expirationdate), '%Y-%m-%d') > CURDATE() 
                                AND STR_TO_DATE(FROM_UNIXTIME(s.update_expirationdate), '%Y-%m-%d') > CURDATE(),
                                'true',
                                'false'
                            ) AS status,
                            FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') as created_long
                                from
                                    serials s
                                 where FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') > '{last_sync[0][0]}'
                                order by
                                    s.date asc
            """
            new_serials = db.legacy_db(serials_query,None)
            if len(new_serials) ==0 :
                print("DB: No New Serials")
            else:
                serial_list = ', '.join([f"'{serial[1]}'" for serial in new_serials])
                # Query to retrieve contact hubspot ids
                hubspotids_query = f"""select email,"hubspotID" from contacts 
                where email in ({serial_list})"""
                hubspotids = db.analytics_db("GET",hubspotids_query, None)
                if hubspotids == []:
                    print("NOTE: Serial contacts not yet added to hubspot")
                else:
                    # Create a dictionary mapping email addresses to hubspot IDs
                    contacts_dict = {email: hubspot_id for email, hubspot_id in hubspotids}
    except Exception as get_exception:
        print(f"Error in Serials (GET): {get_exception}")
    return (new_serials,contacts_dict)

def get_updated_serials():
    """
    Retrieve updated serials from the database based on the last synchronization timestamp.
    Run this before updating associations because they both rely on the updated column

    Returns:
        list: A list of dictionaries representing the updated serials.
    """
    print("START: Getting updated serials")
    final = []
    try:
        # Query to retrieve the minimum updated timestamp from the database
        query = "select max(created) from serials"
        last_sync = db.analytics_db( "GET",query, None)
        # Query to retrieve updated serials from the database
        query = f"""
                    select  s.serial,
                    s.email,
                    FROM_UNIXTIME(s.date, '%Y-%m-%d') as created,
                    s.id,
                    s.maxUse,
                    FROM_UNIXTIME(s.update_expirationdate, '%Y-%m-%d') as update_expirationdate,
                    IF(
                        STR_TO_DATE(FROM_UNIXTIME(s.expirationdate), '%Y-%m-%d') > CURDATE() 
                        AND STR_TO_DATE(FROM_UNIXTIME(s.update_expirationdate), '%Y-%m-%d') > CURDATE(),
                        'true',
                        'false'
                    ) AS status,
                    FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') as created_long
        from
            serials s
            where last_updated_on > '{last_sync[0][0]}'
            and 'deletedAt' is  null
        """
        updated_serials = db.legacy_db( query, None)
        print("SUCCESS: Updated serials retrieved from DB")
        serial_list = ', '.join([f"'{serial[0]}'" for serial in updated_serials])
        if serial_list =="":
            print("No New Serials")
        else:
            # Query to retrieve contact hubspot ids
            hubspotids_query = f"""select serial,"hubspotID" from serials
            where serial in ({serial_list})"""
            hubspotids = db.analytics_db("GET",hubspotids_query, None)
        # Create a dictionary mapping email addresses to hubspot IDs
            hubspot_dict = {serial: hubspot_id for serial, hubspot_id in hubspotids}
            # Join the two lists based on email addresses
            for serial in updated_serials:
                serialid = serial[0]  # Assuming email is at index 1 in the new_serials tuples
                hubspot_id = hubspot_dict.get(serialid, None)
                if hubspot_id is not None:
                    final.append((*serial, hubspot_id))
    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return final

def insert_serial_ids(hubspotids):
    """
    Insert new HubSpot IDs for serials into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, serial, and created date.

    Returns:
        None
    """
    print("START: Inserting new serial HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = 'insert into serials ("hubspotID", serial, created) values (%s, %s, %s)'
        # Iterate over the provided HubSpot IDs
        for serial in hubspotids:
            values = (int(serial[0]), serial[1], serial[2])
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New serial HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (serials): {get_exception}")

def delete_serial_ids():
    """
    Delete HubSpot IDs associated with serials from the database.

    Args:
        serials (list): A list of serials to be deleted.

    Returns:
        True and False
    """
    print("START: Deleting serials on DB")
    hubspot_ids = []
    try:
        query = "select max(created) from serials"
        last_sync = db.analytics_db( "GET",query, None)
        query = f"""
                    select  s.serial  from
            serials s
            where "deletedAt" > '{last_sync[0][0]}'
        """
        deleted_serials = db.legacy_db( query, None)
        if len(deleted_serials)>0:
            # Query to retrieve HubSpot IDs for the provided serials
            serial_list = ', '.join([f"'{serial[0]}'" for serial in deleted_serials])
            hubspot_query = f'select "hubspotID" from serials where serial in ({serial_list})'
            hubspot_ids = db.analytics_db("GET", hubspot_query, None)
            query = f'delete from serials where serial in ({serial_list})'
            db.analytics_db("DELETE", query, None)
            print("SUCCESS: Serial IDs deleted from the database")
        else:
            print("No serials to be deleted")
    except Exception as get_exception:
        print(f"Error in Serials (DELETE): {get_exception}")
    return hubspot_ids

def get_workspaces(subscriptions=None,customers_list=None):
    """
    Retrieve and process workspace data from different databases.

    Args:
        subscriptions (list): List of subscription data.
        customers_list (list): List of customer IDs.

    Returns:
        list: List of tuples containing workspace information.
            Each tuple format: (id, name, identifier, createdAt, customerId, ...other columns)
    """
    # Print a message indicating the start of the retrieval process
    print("START: retrieving workspaces from DB")
    final_workspaces = []
    # Fetch the last sync timestamp from the database
    if subscriptions is not None:
        try:
            subscriptions_df = pd.DataFrame(subscriptions, columns=[
                'subscription_id','created', 'customer','ended_at',
                'plan_id','plan','quantity','status',
                'trial_start','trial_end','current_period_end','email','priority','payment_menthod','auto_renew'])
            # Create a comma-separated string of workspace customer IDs for the next query
            customers_string = ', '.join([f"'{customer}'" for customer in customers_list])
            # Query to retrieve customer IDs from the payments database
            query = f"""
                        SELECT 
                            id as payments_id, 
                            external_id
                        FROM customers 
                        WHERE external_id IN ({customers_string})
                        """
            workspace_customers = db.payments_db(query, None)
            workspace_customers_df = pd.DataFrame(workspace_customers, 
                                                  columns=['payments_id', 'external_id'])

            print(f"workspace_customers:{len(workspace_customers)}")

            payments_string = ', '.join([f"'{id}'" for id,customer in workspace_customers])
            query = f"""
                        SELECT 
                            id, 
                            name,
                            identifier,
                            "createdAt",
                            "customerId"
                        FROM "Organizations" 
                        WHERE "customerId" in ({payments_string})
                        ORDER BY "createdAt" ASC
                        """
            workspaces = db.cloud_db(query, None)
            workspaces_df = pd.DataFrame(workspaces, columns=
                        ['id', 'name', 'identifier', 'createdAt', 'customerId'])
            # Perform the joins
            result_df = subscriptions_df.merge(workspace_customers_df,
                                 left_on='customer', right_on='external_id')
            result_df = result_df.merge(workspaces_df, 
                                left_on='payments_id', right_on='customerId')
            # Extract the relevant columns
            final_workspaces = result_df[['id', 'name', 'identifier','customer','email'
                        ,'subscription_id','created','plan_id','status','ended_at','plan',
                        'quantity','trial_start','trial_end','current_period_end','createdAt',
                        'priority','payment_menthod','auto_renew']]
        except Exception as get_exception:
            # Handle exceptions and print an error message
            print(f"Error in Workspaces (GET): {get_exception}")
    else:
        try:
            # Query to retrieve the last updated memberships from the analytics database
            query = "SELECT MAX(created) FROM workspaces"
            last_sync = db.analytics_db("GET", query, None)
            if last_sync[0][0] is None:
                print(""" WORKSPACES TABLE EMPTY: PLEASE RUN -- 
                `python main.py create_all_workspaces` -- TO TRIGGER A FULL SYNC""")
            else:
                # Query to retrieve new workspaces from the Organizations table 
                query = f"""
                            SELECT 
                                id, 
                                name,
                                identifier,
                                "createdAt",
                                "customerId"
                            FROM "Organizations" 
                            WHERE "createdAt" > '{last_sync[0][0]}' and "deletedAt" is null
                            ORDER BY "createdAt" ASC 
                            """
                workspaces = db.cloud_db(query, None)
                if len(workspaces) > 0:
                    # Create a comma-separated string of workspace customer IDs for the next query
                    workspaces_list = ', '.join([f"'{workspace[4]}'" for workspace in workspaces 
                                                 if workspace[4] is not None])
                    # Query to retrieve customer IDs from the payments database
                    query = f"""
                                SELECT 
                                    id, 
                                    external_id
                                FROM customers 
                                WHERE id IN ({workspaces_list})
                                """
                    workspace_customers = db.payments_db(query, None)

                    # Create a dictionary mapping payment IDs to external IDs
                    customers_dict = {id: external_id for id, external_id in workspace_customers}

                    # Join workspace data and customer IDs based on payment IDs
                    for workspace in workspaces:
                        payments_id = workspace[4]
                        customer_id = customers_dict.get(payments_id, None)
                        if customer_id is not None:
                            final_workspaces.append((*workspace, customer_id))
        except Exception as get_exception:
            # Handle exceptions and print an error message
            print(f"Error in Workspaces (GET): {get_exception}")
        print(f"final_workspaces:{len(final_workspaces)}")
    return final_workspaces

def get_workspace_hubspot_id(customer):
    """
    Retrieve HubSpot ID associated with a specific customer's workspace.

    Args:
        customer (str): Customer ID.

    Returns:
        str: HubSpot ID associated with the customer's workspace.
    """
    query = f"""select "hubspotID" from workspaces where customer = '{customer}'"""
    hubspotid = db.analytics_db("GET",query, None)
    return hubspotid

def add_contact_hubspot_id(workspaces):
    """
    Add HubSpot ID to the workspace data.

    Args:
        workspaces (list): List of workspace tuples.

    Returns:
        list: List of tuples containing updated workspace information.
    """
    print("START: Getting contact associations")
    final_workspaces = []
    contacts_list =  ', '.join([f"'{workspace[3]}'" for workspace in workspaces 
                                if workspace[3] is not None])
    query = f"""select email,"hubspotID" from contacts where email in ({contacts_list}) and  email NOT ILIKE '%''%'
              ESCAPE '#'"""
    hubspot_ids = db.analytics_db("GET",query, None)

    # Create a dictionary mapping payment IDs to external IDs
    contacts_dict = {email: hubspot_id for email, hubspot_id in hubspot_ids}

    # Join workspace data and customer IDs based on payment IDs
    for workspace in workspaces:
        email = workspace[3]
        hubspot_id = contacts_dict.get(email, None)
        if hubspot_id is not None:
            final_workspaces.append((*workspace, hubspot_id))
    print("SUCCESS: Contact associations retrieved")
    return final_workspaces

def insert_workspace_ids(hubspotids):
    """
    Insert new HubSpot IDs for Workspaces into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, SketchID, and created date.

    Returns:
        None
    """
    print("START: Inserting new Workspaces HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = """insert into workspaces ("hubspotID", workspace,customer, created) 
        values (%s, %s,%s, %s)"""
        # Iterate over the provided HubSpot IDs
        for workspace in hubspotids:
            # Remove trailing zeros from fractional seconds
            if len(workspace[2])>19:
                timestamp_string = '.'.join(workspace[2].split('.')[:-1])
                # Parse the string into a datetime object
                timestamp_format = '%Y-%m-%dT%H:%M:%S'
                timestamp_datetime = datetime.datetime.strptime(timestamp_string, timestamp_format)

                # Format the datetime object as a date
                date = timestamp_datetime.strftime('%Y-%m-%d %H:%M:%S')
            else:
                date =workspace[2]
            values = (int(workspace[0]), workspace[1], workspace[4], date)
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New Workspace HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (Workspaces): {get_exception}")
    return True

def delete_workspace_ids(query=None):
    """
    Delete HubSpot IDs associated with workspaces from the database.

    Returns:
        tuple: A tuple containing deleted HubSpot IDs and corresponding workspace IDs.
    """
    print("START: Deleting workspaces on DB")
    hubspot_ids = []
    workspace_list = []
    if query is None:
        try:
            query = "select max(created) from workspaces"
            last_sync = db.analytics_db( "GET",query, None)
            if last_sync[0][0] is not None:
                query = f"""
                                    SELECT 
                                        id
                                    FROM "Organizations" 
                                    WHERE "deletedAt" > '{last_sync[0][0]}' 
                                    """
                workspaces = db.cloud_db(query, None)
                if len(workspaces) > 0:
                    workspace_list = ', '.join([f"'{workspace[0]}'" for workspace in workspaces])                 
                else:
                    print("No workspaces to be deleted")
            else:
                print("No workspaces to be deleted")
        except Exception as get_exception:
            print(f"Error in Workspaces (DELETE): {get_exception}")
        return (hubspot_ids,workspace_list)
    else:
        hubspot_query = query
        hubspot_ids = db.analytics_db("GET", hubspot_query, None)
        if len(hubspot_ids) > 0:
            query = f'delete from workspaces where workspace in ({workspace_list})'
            db.analytics_db("DELETE", query, None)
            print("SUCCESS: Workspace IDs deleted from the database")
        else:
            print("No workspaces to be deleted")

def get_memberships():
    """
    Retrieves new organization memberships from the database.

    Returns:
        list: A list of new organization memberships.
    """
    print("START: Getting new memberships")
    contacts_dict = []
    workspaces_dict = []
    new_memberships = []
    # Query to retrieve the last created membership
    query = "select max(created) from memberships"
    last_sync = db.analytics_db("GET",query, None)
    if last_sync[0][0] is None:
        print("Memberships table empty, Running full sync")
        try:
            query = 'select workspace,"hubspotID" from workspaces'
            workspaces = db.analytics_db("GET",query, None)
            if len(workspaces)>0:
                query = 'select member from memberships'
                memberships = db.analytics_db("GET",query, None)
                memberships_list = ', '.join([f"'{member[0]}'" for member in memberships])

                workspaces_list = ', '.join([f"'{workspace[0]}'" for workspace in workspaces])
                # Query to retrieve new memberships from the database
                query = f"""
                SELECT 
                    om.id, 
                    om."UserId", 
                    om."OrganizationId", 
                    om.role, 
                    case 
                    when om."isPrimary" = True
                    then 'yes'
                    else 'no'
                    end as "isPrimary",
                    case 
                    when om."isContributor" = True
                    then 'yes'
                    else 'no'
                    end as "isContributor",
                    om."createdAt", 
                    u.email 
                    FROM "OrganizationMemberships" om
                    LEFT JOIN "Users" u on (u.id = om."UserId")
                    where om."OrganizationId" in ({workspaces_list}) and role != 'guest' and  om.id not in ({memberships_list})
                    order by om."createdAt" asc 
                """
                new_memberships = db.cloud_db( query, None)
                print(f"Memberships: {len(new_memberships)}")
                # Create a dictionary mapping workspace ids to hubspot IDs
                workspaces_dict = {workspace: hubspot_id for workspace, hubspot_id in workspaces}
                # Query to retrieve contact hubspot ids
                email_list_str = []
                for member in new_memberships:
                    email = member[7]
                    if email is not None:
                        email_list_str.append(email.replace("'", ""))
                email_list = ', '.join([f"'{member}'" for member in email_list_str])
                if email_list =="":
                    print("DB: No New Memberships")
                else:
                    # Query to retrieve contact hubspot ids
                    hubspotids_query = f"""select email,"hubspotID" from contacts
                            where email in ({email_list}) and email NOT ILIKE '%''%' ESCAPE '#'"""
                    hubspotids = db.analytics_db("GET",hubspotids_query, None)
                    if hubspotids == []:
                        print("NOTE: Membership contacts not yet added to hubspot")
                    else:
                        # Create a dictionary mapping email addresses to hubspot IDs
                        contacts_dict = {email: hubspot_id for email, hubspot_id in hubspotids}
            else:
                print("""WORKSPACES TABLE EMPTY: PLEASE RUN -- 
                `python main.py create_all_workspaces`
                            -- TO TRIGGER A FULL SYNC""")
        except Exception as get_exception:
            print(f"Error in Serials (GET): {get_exception}")
    else:
        try:
            # Query to retrieve new memberships from the database
            query = f"""
            SELECT 
                om.id, 
                om."UserId", 
                om."OrganizationId", 
                om.role, 
                case 
                    when om."isPrimary" = True
                    then 'yes'
                    else 'no'
                    end as "isPrimary",
                case 
                    when om."isContributor" = True
                    then 'yes'
                    else 'no'
                    end as "isContributor", 
                om."createdAt", 
                u.email 
                FROM "OrganizationMemberships" om
                LEFT JOIN "Users" u on (u.id = om."UserId")
                where om."createdAt" > '{last_sync[0][0]}'
                order by om."createdAt" asc 
            """
            new_memberships = db.cloud_db( query, None)
            #get workspace hubspot ids
            workspaces_list = ', '.join([f"'{member[2]}'" for member in new_memberships])
            if workspaces_list =="":
                print("DB: No New Memberships")
            else:
                # Query to retrieve workspace hubspot ids
                hubspotids_query = f"""select workspace,"hubspotID" from workspaces 
                                                where workspace in ({workspaces_list})"""
                hubspotids = db.analytics_db("GET",hubspotids_query, None)
                if hubspotids == []:
                    print("NOTE: workspaces not yet added to hubspot")
                else:
                    # Create a dictionary mapping workspace ids to hubspot IDs
                    workspaces_dict = {workspace: hubspot_id for workspace, hubspot_id 
                                       in hubspotids}
                # Query to retrieve contact hubspot ids
            email_list_str = []
            for member in new_memberships:
                email = member[7]
                if email is not None:
                    email_list_str.append(email.replace("'", ""))
            email_list = ', '.join([f"'{member}'" for member in email_list_str])
            if email_list =="":
                print("DB: No New Memberships")
            else:
                # Query to retrieve contact hubspot ids
                hubspotids_query = f"""select email,"hubspotID" from contacts where email in
                  ({email_list})
                and email NOT ILIKE '%''%' ESCAPE '#' """
                hubspotids = db.analytics_db("GET",hubspotids_query, None)
                if hubspotids == []:
                    print("NOTE: Membership contacts not yet added to hubspot")
                else:
                    # Create a dictionary mapping email addresses to hubspot IDs
                    contacts_dict = {email: hubspot_id for email, hubspot_id in hubspotids}
        except Exception as get_exception:
            print(f"Error in Memberships (GET): {get_exception}")
    return (new_memberships,contacts_dict,workspaces_dict)

def insert_membership_ids(hubspotids):
    """
    Insert new HubSpot IDs for memberships into the database.

    Args:
        hubspotids (list): A list of tuples containing HubSpot ID, membershipID, and created date.

    Returns:
        None
    """
    print("START: Inserting new Memberships HubSpot IDs")
    try:
        # Define the SQL query for inserting data
        query = 'insert into memberships ("hubspotID", member, created) values (%s, %s, %s)'
        # Iterate over the provided HubSpot IDs
        for membership in hubspotids:
            values = (int(membership[0]), membership[1], membership[2])
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New Memberships HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (Memberships): {get_exception}")

def get_updated_memberships():
    """
    Retrieves updated organization memberships from the database.

    Returns:
        list: A list of updated organization memberships.
    """
    print("START: Get updated memberships")
    final =[]
    try:
        # Query to retrieve the last updated memberships
        query = "select max(created) from memberships"
        last_sync = db.analytics_db("GET",query, None)
        if last_sync[0][0] is not None:
            # Query to retrieve updated memberships from the database
            query = f"""
            SELECT 
                om.id, 
                om."UserId", 
                om."OrganizationId", 
                om.role, 
                case 
                    when om."isPrimary" = True
                    then 'yes'
                    else 'no'
                    end as "isPrimary", 
                case 
                    when om."isContributor" = True
                    then 'yes'
                    else 'no'
                    end as "isContributor",   
                om."createdAt", 
                u.email 
                FROM "OrganizationMemberships" om
                LEFT JOIN "Users" u on (u.id = om."UserId")
                where om."updatedAt" > '{last_sync[0][0]}'
            """
            memberships = db.cloud_db( query, None)

            memberships_list = ', '.join([f"'{member[0]}'" for member in memberships])
            if memberships_list !="":
                # Query to retrieve members hubspot ids
                hubspotids_query = f"""select member,"hubspotID" from memberships 
                                        where member in ({memberships_list})"""
                hubspotids = db.analytics_db("GET",hubspotids_query, None)
            # Create a dictionary mapping sketchids  to hubspot IDs
                hubspot_dict = {member: hubspot_id for member, hubspot_id in hubspotids}
                # Join the two lists based on sketchids
                for member in memberships:
                    sketchid = member[0]  # Assuming email is at index 1 in the new_serials tuples
                    hubspot_id = hubspot_dict.get(sketchid, None)
                    if hubspot_id is not None:
                        final.append((*member, hubspot_id))
        else:
            print("No Memberships to be updated")

    except Exception as get_exception:
        print(f"Error in Serials (UPDATED): {get_exception}")
    return final

def delete_memberships(payload):
    """
    Delete HubSpot IDs associated with memberships from the database.

    Args:
        memberships (list): A list of memberships to be deleted.

    Returns:
        True and False
    """
    print("START: Deleting memberships on DB")
    hubspot_ids = []

    try:
        if len(payload)>0:
            query = f"""
                    SELECT 
                        id
                    FROM "OrganizationMemberships" 
                    WHERE "OrganizationId" in ({payload}) 
                    """
            memberships = db.cloud_db(query, None)
            if len(memberships) > 0:
                memberships_list = ', '.join([f"'{member[0]}'" for member in memberships])
                hubspot_query = f"""select "hubspotID" from memberships 
                                    where member in ({memberships_list})"""
                hubspot_ids = db.analytics_db("GET", hubspot_query, None)
                if len(hubspot_ids) > 0:
                    query = f'delete from memberships where member in ({memberships_list})'
                    db.analytics_db("DELETE", query, None)
                    print("SUCCESS: Memberships IDs deleted from the database")
                else:
                    print("No Memberships to be deleted")
            else:
                print("No Memberships to be deleted")
        else:
            print("No Memberships to be deleted")
    except Exception as get_exception:
        print(f"Error in Workspaces (DELETE): {get_exception}")
    return hubspot_ids

def workspace_associations():
    """
    Fetches and associates workspace-related data using multiple database queries.

    Returns:
        list: A list of tuples containing associated workspace data.
    """
    try:
        # Initialize lists to store data
        workspace_email = []
        complete = []

        # Query to fetch workspaces created after a certain date
        query = """SELECT w."hubspotID", w.workspace, w.customer as workspace 
                        FROM workspaces w where w.created > '2023-12-17' """
        workspaces = db.analytics_db("GET", query, None)

        # Print retrieved workspaces data
        print(workspaces)

        # Create a string of workspace names for query use
        workspaces_list = ', '.join([f"'{member[2]}'" for member in workspaces])

        # Query to fetch data from another database based on workspace names
        analytics_query = f"""SELECT id, email FROM ab_stripe.customers  
        WHERE id IN ({workspaces_list})"""
        workspaces_data = db.analytics_db("GET", analytics_query, None)

        # Print analytics query
        print(analytics_query)

        # Create a dictionary mapping payment IDs to external IDs
        workspaces_dict = {id: external_id for id, external_id in workspaces_data}

        # Join workspace data and customer IDs based on payment IDs
        for workspace in workspaces:
            id = workspace[2]
            email = workspaces_dict.get(id, None)
            if email is not None:
                workspace_email.append((*workspace, email))

        # Create a string of emails for query use
        email_list = ', '.join([f"'{email[3]}'" for email in workspace_email])
        print(email_list)

        # Query to fetch hubspot email and IDs based on emails
        query = f"""SELECT email, "hubspotID" FROM contacts WHERE email IN ({email_list})"""
        hubspot_email = db.analytics_db("GET", query, None)

        # Create a dictionary mapping emails to IDs
        email_dict = {email: id for email, id in hubspot_email}

        # Join workspace data and customer IDs based on emails
        for record in workspace_email:
            email = record[3]
            id = email_dict.get(email, None)
            if id is not None:
                complete.append((*record, id))

    except Exception as get_exception:
        print(f"Error in Workspaces (ASSOCIATE): {get_exception}")
    # Print and return the completed associations
    return complete


def memberships_associations():
    """
    Fetches and associates membership-related data using multiple database queries.

    Returns:
        list: A list of tuples containing associated membership data.
    """
    try:
        # Initialize lists to store data
        membership_email = []
        membership_workspace = []
        membership_all = []

        # Query to fetch memberships created after a certain date
        query = """SELECT "hubspotID", member as id FROM memberships 
                                    WHERE created > '2023-12-17' """
        memberships = db.analytics_db("GET", query, None)
        memberships_list = ', '.join([f"'{member[1]}'" for member in memberships])
        # Query to fetch membership data from another database
        analytics_query = f"""SELECT mem.id, u.email, mem."OrganizationId"
                            FROM cloud."OrganizationMemberships" mem
                            LEFT JOIN cloud."Users" u ON mem."UserId" = u.id
                            WHERE mem.id IN ({memberships_list}) AND u.email 
                            NOT ILIKE '%''%' ESCAPE '#'"""
        memberships_data = db.analytics_db("GET", analytics_query, None)
        memberships_all_dict = {member[1]: member[0] for member in memberships}

        # Join membership data and IDs based on payment IDs
        for member in memberships_data:
            id = member[0]
            hub_id = memberships_all_dict.get(id, None)
            if hub_id is not None:
                membership_all.append((*member, hub_id))

        # Create a string of emails for query use
        email_list = ', '.join([f"'{email[1]}'" for email in memberships_data])
        query = f"""SELECT email, "hubspotID" FROM contacts WHERE email IN ({email_list})"""
        hubspot_email = db.analytics_db("GET", query, None)
        # Create a string of workspaces for query use
        workspace_list = ', '.join([f"'{org[2]}'" for org in memberships_data])
        query = f"""SELECT workspace, "hubspotID" FROM workspaces 
                                WHERE workspace IN ({workspace_list})"""
        hubspot_workspace = db.analytics_db("GET", query, None)

        # Create dictionaries to map emails and workspaces to IDs
        memberships_email_dict = {member[0]: member[1] for member in hubspot_email}
        memberships_workspace_dict = {member[0]: member[1] for member in hubspot_workspace}

        # Join membership data and customer IDs based on emails
        for member in membership_all:
            id = member[1]
            email = memberships_email_dict.get(id, None)
            if email is not None:
                membership_email.append((*member, email))

        # Join membership data and workspaces based on IDs
        for member in membership_email:
            id = member[2]
            workspace = memberships_workspace_dict.get(id, None)
            if workspace is not None:
                membership_workspace.append((*member, workspace))
    except Exception as get_exception:
        print(f"Error in Workspaces (ASSOCIATE): {get_exception}")
    
    # Return the completed associations
    return membership_workspace