""" Contains all hubspot operations"""
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
            print("Contacts table empty, strart full Sync")
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
            user_query = """select email, "createdAt" from "Users" where email is not null 
            group by 1,2 order by "createdAt" asc limit 4000"""
            cloud_users = db.cloud_db(user_query,None)
            cloud_users = [(email, date, 'cloud') for email, date in cloud_users]

            sso_query = """select email, "createdAt" from "ExternalIdentities"
            where email is not null group by 1,2 order by "createdAt" asc """
            sso_users = db.cloud_db(sso_query,None)
            sso_users = [(email, date, 'sso') for email, date in sso_users]

            user_licenses = '''select email, max(FROM_UNIXTIME(date)) as date
            from serials group by 1 limit 200'''
            serial_users = db.legacy_db(user_licenses,None)
            serial_users = [(email, date, 'serial') for email, date in serial_users]
        else:
            # Fetch new contacts from Users table
            last_sync_query = "select max(created) from contacts where  type = 'cloud'"
            last_sync_timestamp = db.analytics_db("GET",last_sync_query, None)
            user_query = """select email, max("createdAt") as "createdAt" from "Users" where email
            is not null and "createdAt" > %s group by 1  order by "createdAt" asc limit 2000"""
            cloud_users = db.cloud_db(user_query,last_sync_timestamp[0])
            cloud_users = [(email, date, 'cloud') for email, date in cloud_users]

            last_sync_query = "select max(created) from contacts where  type = 'sso'"
            last_sync_timestamp = db.analytics_db("GET",last_sync_query, None)
            sso_query = """select email, "createdAt" from "ExternalIdentities" 
            where email is not null and "createdAt" > %s group by 1,2 order by "createdAt" asc """
            sso_users = db.cloud_db(sso_query,last_sync_timestamp)
            sso_users = [(email, date, 'sso') for email, date in sso_users]

            #fetch new users from Serials
            last_sync_query = "select max(created) from serials"
            last_sync = db.analytics_db("GET",last_sync_query, None)
            unix_timestamp = int(last_sync[0][0].timestamp())
            user_licenses = f"""select email,FROM_UNIXTIME(date) as date from serials 
            where date > '{unix_timestamp}' limit 300 """
            serial_users = db.legacy_db(user_licenses,None)
            serial_users = [(email, date, 'serial') for email, date in serial_users]

            print("SUCCESS: New contacts retrieved from Database")
    except Exception as get_exception:
        print(f"Error in Contacts (GET): {get_exception}")
    return cloud_users+serial_users+sso_users

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
        joined_list = []
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
                                'Active',
                                'Canceled'
                            ) AS status,
                            FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') as created_long
                            from
                                serials s
                            order by
                                s.date asc limit 400
        """
            all_serials = db.legacy_db(serials_query,None)
            serial_emails = ', '.join([f"'{serial[1]}'" for serial in all_serials])
            hubspotids = db.analytics_db("GET",f'''
                    select email,"hubspotID" from contacts where email in ({serial_emails})
                    ''', None)
            # Create a dictionary mapping email addresses to hubspot IDs
            hubspot_dict = {email: hubspot_id for email, hubspot_id in hubspotids}

            # Join the two lists based on email addresses
            for serial in all_serials:
                email = serial[1]  # Assuming email is at index 1 in the new_serials tuples
                hubspot_id = hubspot_dict.get(email, None)
                if hubspot_id is not None:
                    joined_list.append((*serial, hubspot_id))
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
                                'Active',
                                'Canceled'
                            ) AS status,
                            FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') as created_long
                                from
                                    serials s
                                 where FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') > '{last_sync[0][0]}'
                                order by
                                    s.date asc
                                limit
                                    50
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
                    hubspot_dict = {email: hubspot_id for email, hubspot_id in hubspotids}

                    # Join the two lists based on email addresses
                    for serial in new_serials:
                        email = serial[1]  # Assuming email is at index 1 in the new_serials tuples
                        hubspot_id = hubspot_dict.get(email, None)
                        if hubspot_id is not None:
                            joined_list.append((*serial, hubspot_id))
    except Exception as get_exception:
        print(f"Error in Serials (GET): {get_exception}")
    return joined_list

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
                        'Active',
                        'Canceled'
                    ) AS status,
                    FROM_UNIXTIME(s.date, '%Y-%m-%d %h:%i:%s') as created_long
        from
            serials s
            where last_updated_on > '{last_sync[0][0]}'

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
            print(serial)
            values = (int(serial[0]), serial[1], serial[2])
            db.analytics_db("UPDATE", query, values)
        print("SUCCESS: New serial HubSpot IDs inserted")
    except Exception as get_exception:
        print(f"Error in insertHubspotID (serials): {get_exception}")

def delete_serial_ids(serials):
    """
    Delete HubSpot IDs associated with serials from the database.

    Args:
        serials (list): A list of serials to be deleted.

    Returns:
        True and False
    """
    print("START: Deleting serials on DB")
    try:
        # Query to retrieve HubSpot IDs for the provided serials
        serial_list = ', '.join([f"{serial}" for serial in serials])
        query = f'delete from serials where "hubspotID" in ({serial_list})'
        db.analytics_db("DELETE", query, None)
        print("SUCCESS: Serial IDs deleted from the database")
    except Exception as get_exception:
        print(f"Error in Serials (DELETE): {get_exception}")
    return True
